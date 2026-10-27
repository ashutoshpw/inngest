# Ordinary StepRun response input binding (test-only Go evidence; accepted)

## Scope and worktree

Parent-authorized test-only implementation, accepted after independent Luna
review02 found no blockers and the parent accepted the narrowed contract.
Assigned T3 worktree `/home/ashutosh/.t3/worktrees/inngest/t3-235214f9`, branch
`t3/test-step-run-input-binding`. Initially clean, HEAD `a4592cde4`, with
`git rev-list --left-right --count HEAD...4a870dd35e2494c0b3b0f95b97a24ed3021bd674`
reporting `0 35`. `git merge --ff-only 4a870dd35e2494c0b3b0f95b97a24ed3021bd674`
advanced this checkout in place to the requested PR19 base. No T3 binding change.

This is a **prospective test-side contract**, not a production adapter, Go/native
response owner, eligibility proof, transport or routing/fallback implementation.
Only ordinary single non-AI StepRun persistence/discovery inputs are bound;
this is not an exhaustive inventory of every Go response/tracing input. Nil/invalid/error/AI descriptors,
legacy Step, StepError/Failed/Planned, None, mixed/multiple-op responses, Sleep,
wait/invoke/gateway/defer/completion and all other opcodes remain outside this
contract. Existing broader tests continue calling their existing owners.
Batch/multi-event/native adapter admission and queue policy qualifications remain
with their existing references. No new production eligibility selector exists.

## Sources, proposals and observations

`response_input_binding_test.go` defines private `stepRunInputFacts` and
`bindStepRunInputs`. The inputs are the actual executor settings, explicit
`HandleGeneratorResponse` response and actual RunContext, never fixture expected
values or the staged store's configured outcomes:

- `loaded`: detached original metadata ID/config/metrics/stack; StateSize and both
  metadata-size counters are loaded facts. RequestVersion/ForceStepPlan remain
  stale for this response even after a successful metadata proposal.
- `original`: original opcode descriptor, including raw Data, options, timing,
  userland and scoped metadata; never `Output()` or prepared bytes. Raw Data and
  raw-message options are cloned verbatim, including malformed data. Other
  descriptor fields and metadata/item snapshots use JSON cloning with UseNumber
  for interface values. These are test representations of exported source values,
  not a new wire encoding or a general clone of arbitrary Go object graphs/mutexes.
- Explicit response version and NoRetry/RetryAt; a **JSON projection** of the
  lifecycle queue item and its actual payload edge (not `runInstance.edge`,
  which can differ); incoming context group. This is not a full lifecycle item
  snapshot. `JobID`, `EnqueuedAt`, `At` and runtime handles are excluded.
- Actual `stateSizeLimit` callback handle, preserving nil versus configured and
  zero-return/default behavior. Binding
  does not call the resolver. Resolution happens in the unchanged Go preparation
  owner and can have effects. No portable/native numeric-limit ABI is claimed.

No preparedOutput, saveCompleted, hasPending, shouldEnqueue or completed recipe
is an initial binding field. No metadata proposal is substituted for `loaded`.
No bound field steers production execution; the original actual objects still
enter the unchanged owner. Thus these checks qualify prospective input sources,
not production consumption of the new test type.

`stagedBatchStore.saves` is the only new shared recorder: under its existing lock
it records actual SaveStep run ID, step ID, output bytes, context metadata delta
and the exact returned pending/error pair. It forwards the existing configured
outcome unchanged. That outcome is a test port return, not durable store truth.
Existing store snapshots own metadata proposals/pending calls, and existing queue,
lifecycle, clock, span and callback recorders own other outputs. Real persistence
truth stays with the existing fresh-store-reader witnesses.

The 40-case Go corpus invokes the binding only for ordinary single StepRun at
its existing owner call; it does not replay a second corpus or generate expected
outputs. Its existing literal assertions remain policy authority. The harness
callback now receives the active subtest `*testing.T`; the tagged native corpus
call has a signature-only forwarding closure to stay compatible. No native test
execution or Rust change is part of this work.

## Authoring gate and distinct missing proof

1. Protect source provenance, snapshot isolation and separation of source facts
   from effect answers at the actual response boundary.
2. Credible regressions: reading `i.resp` instead of the explicit response,
   canonicalizing raw input into prepared bytes, aliasing queue pointers, resolving
   settings eagerly, refreshing loaded metadata from a proposal, or replacing an
   actual pending/error return with a pre-bound answer.
3. Existing corpus/persistence tests own outputs and policy; they did not expose
   a prospective input binding or qualify its source selection. The new seven
   source-change cases reuse the accepted preparation literals as their byte
   oracle. They change actual function identity/idempotency, edge, priority,
   max attempts, loaded state/metadata counters and versions. Explicit response
   version 3 is deliberately different from RunContext response version 99;
   original loaded version -1 remains -1 while the actual proposal is 3. Returned
   output and queue identity/edge/priority reflect the changed sources. For the
   missing-data case, the save answer changes **after binding** to pending=true
   plus a wrapped duplicate: the real return remains intact and suppresses enqueue.
   Other cases expose actual save bytes/delta and generated queue observations.
   Later pointer/data/version edits cannot rewrite the bound snapshot.
4. No production seam. No copied planner, algorithmic expected output generator,
   new fixtures, dependency, production option or export.

## Activation sites and retained witnesses

Binding must observe zero resolver calls, clock reads and exported spans. The
source-change cases use the real OTel provider and existing continuationClock;
post-owner logs retain actual clock counts, exported span counts and actual save/
queue values (including generated groups/carriers). Nonempty actual spans are
required even for ordinary non-AI StepRun; queued cases require the real carrier.
Generated UUIDs are validated/compared after execution; none is injected or
precomputed. Suppressed enqueue has no queue carrier observation: this does not
assert that all tracing effects are absent or that UUID allocation did not occur.

Do not duplicate these stronger owners:

- `TestResponseContinuationContract` (40 unchanged literals): original metadata,
  proposal before step-count limit, metadata failure, single-StepRun no SavePending,
  pending+save error, duplicate/idempotent continuation versus failure, callback
  suppression and enqueue-error policy, clocks/UUID bindings.
- `TestStagedBatchOutputAndSaveAuthority`, `MetadataDeltaReachesSave`,
  `StateOverflowPrecedesRetryPreferences`, `DiscoveryParallelModeAuthority`,
  `SaveBeforeCapacityRelease`, and `RecipesAndObservationClocks`: Output/state-size,
  real trace/metadata delta transfer, save authority and lifecycle → capacity →
  discovery ordering. Capacity-release observation uses existing direct RunContext
  boundary; it is not newly claimed for a live response capacity lease.
- `TestNativeStepPreparationAccountingThreshold`: actual 34+13 bytes, 47 accepts/
  46 rejects before effects. Seven literals in `native-step-preparation-v1.json`
  remain unchanged.
- [Go persistence baseline](native-step-go-baseline.md): 21 literal SaveStep cases,
  real persistence/store inventory and fresh-reader witnesses. No new store
  boundary was added, so no full Valkey or replay rerun was needed here.
- [Go extraction](response-owner-extraction.md), [continuation](response-continuation.md),
  [real effects](response-real-effects.md), [native enqueue](native-enqueue.md), and
  [bounded memory](bounded-preparation-memory.md) retain prior qualifications.

## Validation and evidence

Evidence directory: `/home/ashutosh/.cache/step-run-input-binding/`.
Go: cached `/home/ashutosh/.cache/go1.26.4/go/bin/go` (1.26.4 linux/amd64).
Every Go command uses `GOTOOLCHAIN=local GOMAXPROCS=2`, both `GOTMPDIR` and `TMPDIR`
`/home/ashutosh/.cache/step-run-input-binding/tmp`, `-p=2`, `-count=1`, `-timeout=120s`.
Exact command tails and log hashes follow.

Retained initial evidence: `first.json` failed on test setup (empty nonnil raw
message for missing input; old helper's fixed function-ID expectation after source
mutation): 41 pass/8 fail test records. `second.json` corrected both and passed
49 records. `third.json` failed six nil-error `require.Same` checks (42 pass/7 fail
including parent); nil errors now use NoError, nonnil errors preserve Same.
These were test-author mistakes, not production regressions. No production fix.

At the initial author-evidence handoff, `scripts/check-plan-status.sh` exited 1
solely for the known unrelated invalid `Implemented — pending staging review` in
`docs/plans/run-ai-debugging.md`; that file is untouched. No full
repository/Valkey/replay suite, Rust invocation or benchmark is part of this
increment.

## Native follow-on STOP

Parent architecture decision required: either pre-effect eligibility proves no
relevant lifecycle/capacity/tracing effects, or one ordered Go handoff preserves
those effects. Neither option is selected/implemented. PR18 still has separate
preparation/save and enqueue adapters orchestrated by tests. Current FFI NO_GO
remains, Phase 1 partial, Phase 2 open, bridge unset. Licensing (SSPL/Apache),
platform/runtime, lifecycle, fencing, activation and performance remain open.
Independent Luna review02 found no blockers, and the parent accepted this
narrowed test-only increment. The remaining native follow-on still requires the
separate parent architecture decision described above.

## Completed author checks (2026-10-27)

Normal `normal.json`: exit 0, 139 test/subtest pass records, zero failures/skips,
package elapsed 0.25s. Race `race.json`: exit 0, 139 pass records, zero
failures/skips, package elapsed 1.524s (excludes dependency-build time).
278 combined final pass records are not 278 unique cases. Superseded pre-final
normal/race logs are retained as `normal-before-nil-options.json` and
`race-before-nil-options.json` (139 passes each); the final binding also preserves
absent options as nil instead of a typed nil interface. Their hashes are in
`log-summary.json`. The unchanged corpus has
40 cases, and the source-change test has seven cases.

Exact final normal command (race command inserts `-race` immediately after
`-json`, and writes `race.json` instead of `normal.json`):

```sh
env GOTOOLCHAIN=local GOMAXPROCS=2 \
  GOTMPDIR=/home/ashutosh/.cache/step-run-input-binding/tmp \
  TMPDIR=/home/ashutosh/.cache/step-run-input-binding/tmp \
  /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -p=2 ./pkg/execution/executor \
  -run '^(TestStepRunInputBinding|TestResponseContinuationContract$|TestResponseContinuationPreparationFailureBeforeRelease$|TestResponseContinuationResetFailureAfterRelease$|TestResponseContinuationCompetingErrorsJoin$|TestStagedBatch|TestNativeStepPreparationAccountingThreshold$|TestNativeStepJobIdentity$|TestHandleGeneratorStep|TestMaybeEnqueueDiscoveryStep|TestDiscoveryEnqueueFailureAlwaysRetries|TestHandleGeneratorSnapshotsScheduledArguments$)' \
  -count=1 -timeout=120s > /home/ashutosh/.cache/step-run-input-binding/normal.json
```

`first.json`, `second.json`, `third.json` used the same environment/binary and
`go test -json -p=2 ./pkg/execution/executor
-run '^TestStepRunInputBindingSourceChanges$|^TestResponseContinuationContract$'
-count=1 -timeout=120s`, with stdout redirected to the respective log.

Four **test-helper-only overlay** mutations (`mutate.py`, exact argv/results in
`mutation-results.json`) each exit 1 with seven subtest failures plus the parent:

- `wrong-response`: bind version from RunContext response; expected 3, actual 99.
- `prepared-as-source`: replace cloned raw Data with `Output()`; expected `null`,
  actual `{"data":null}` (and corresponding independent literals for other cases).
- `eager-limit`: resolve setting during binding; expected zero calls, actual one.
- `aliased-item`: omit item snapshot; later source priority edit rewrites bound
  priority to 88 instead of retaining 19.

Each command uses the environment above and
`go test -json -p=2 -overlay=/home/ashutosh/.cache/step-run-input-binding/NAME-overlay.json
./pkg/execution/executor -run '^TestStepRunInputBindingSourceChanges$'
-count=1 -timeout=120s`, writing `NAME.json`. Overlays never change checkout
files. No mutation exercised a production change. These prove the missing
binding checks can fail for their stated reasons; no production bug is claimed.

`gofmt` was applied to all changed Go test files; `git diff --check` passes.
`git diff --exit-code 4a870dd35e2494c0b3b0f95b97a24ed3021bd674 -- '*.go'
':!**/*_test.go' rust go.mod go.sum ':(glob)**/testdata/**'` exits 0: production Go,
Rust, dependencies and every tracked compatibility/replay fixture are unchanged.
The build-tagged forwarding closure was updated mechanically for the shared
helper signature; tagged native/FFI execution was not rerun.

Local evidence SHA-256 (raw logs retained; not checked into the repository):

```text
9b86d9a99f7e15cbe2f7309f24c248865f0e92c94245ac140b0000111f105cdc  first.json
b3771ca65d0b8da708e2cf4a53f7c1a67f3a1b1d3944e44ef5541d5f2de85653  second.json
966dff2852e24d1aab6ac1c774ced51be675d8bbc5524acfc9d201e5475a9429  third.json
a540d4d35dc90abbd984057de37d598bb0ae2c8b8ef8a16947cdee5c53f3361f  normal.json
e4fac3d72597c74e791469896853389c8140d4af65b21a008867ea7ebbf9b797  race.json
0d058c5227703c806ec10ec49e93597b8bf5a59c961563a228dc9ef21df16f0a  wrong-response.json
ee129378a051425b7bb5b2900387b4daa88279e7e0eb6c16b0c10395bafeb3b8  prepared-as-source.json
3304e0ce4f66b45eff38c384bd6ec9b5a246cfa3355e31139be9eccf18edd61c  eager-limit.json
61a138b0171d32acac2cf40e9ad1fc48d09fd4140384e6c815d5d04970e6c3c6  aliased-item.json
f6f0c32324a0e323612d0cdca62bfc1886b261996cbcb96721a072ee1ac9fc87  mutation-results.json
179a3cff6aaa87fbc177399a40527d7bfb11cfd5d4512a17c6136bb1f0c2f7ea  source-hashes.json
d06d86f060f9f1b96eeb7b796d29404ceb3c1a3d420c9fa4a549e8d39ef36489  log-summary.json
```

Source SHA-256 at validation (full current/base pairs are in `source-hashes.json`):

```text
cd53eb35796fdec203c7fbfb7554aff20e3e7f21b05f254f919022589fae0193  pkg/execution/executor/executor.go
86933644cd421fecf122260625ba62176a60cf9e0a93ba1b0311eddbab3fc064  pkg/execution/executor/response_runner.go
3d4922f732946e231528bd8db8dc3ccb4bb1e74038a24689f89f4dbf3d641e00  pkg/execution/executor/response_effects.go
7ca76eabf42720b4161e7cd4e270555a2e6fe958afac4f9b4b4859076a46100d  pkg/execution/executor/response_queue_effects.go
cefed409d378247d7afca4927f08ffb4b26c76722437d471351935d2412b597b  pkg/execution/executor/run_context.go
951506ea0a786648a47677f076735f6901927fef3fa9af4df09b17962ed9a56e  pkg/execution/executor/response_input_binding_test.go
de955cd17704eba415aaa227e050bbcadde4afe38bdaade9869bb5a5a73639b5  pkg/execution/executor/response_continuation_contract_test.go
5d31a91371543f9e582e707683b28f686296b40bd29c6c73d091fb435de8b8cb  pkg/execution/executor/response_continuation_integration_test.go
b2227e4f45faae15829e785075c2664895dd9cd01d5d702885759a105b223b99  pkg/execution/executor/staged_batch_reference_test.go
d62d8c44d3b5279c66450dfbcb4048e9f84787ee2cc8fe93237d2539ad145ecc  pkg/execution/executor/response_native_baseline_test.go
64d5a88cfe15299daeb0769b99f238b0076f961e023420129536aaa4b2400031  pkg/execution/executor/response_storage_helpers_test.go
e803b2381fbe7d4a2fe9a8dc48e517767b791f5d81b7fbbd7032b7ea6a4febe4  pkg/execution/executor/testdata/compatibility/response-continuation-v1.json
391d5487e36815803a6da76b01e2fa66b32620e351878e895496ed8eac275c14  pkg/execution/executor/testdata/compatibility/native-step-preparation-v1.json
106e5c1f31fa30e9a608f495f8e3db9fdc2595be0299bd41dd9d5cc11fa9fe94  pkg/execution/state/redis_state/testdata/compatibility/native-step-effects-v1.json
877fd609efcadcffd6a273872d8f99549e8e9d81212224c30ce8caf30f8466c5  go.mod
80b2d3fd0fa282345b1fef20263708642fe8fe1b7a14628fadca1d2c7a35feb6  go.sum
a6bfb03581be88ee6bf3e4cd812c09028fa1e10d23698f0a1cbc8bc9590ebb79  rust/Cargo.lock
```


## Bounded review01 repair (2026-10-27; accepted after review02)

Review01 found correct assignments, no production bug, but unprotected source
claims (P2) and an overbroad lifecycle-item label (P3). This repair adds four
`TestStepRunInputBindingResponsePreferences` cases at the actual
`HandleGeneratorResponse` → `SaveStep` failure boundary, without extending the
40-case oracle. Explicit response and `i.resp` deliberately disagree:

- Explicit NoRetry=true versus context false and a retry time: returned error
  preserves the save message and disallows retry, with no RetryAt wrapper.
- Explicit one-hour RetryAt versus context two-hour RetryAt: actual wrapper
  contains the one-hour literal.
- Explicit RetryAt and NoRetry=false versus context NoRetry=true: the returned
  error remains retryable at the explicit time.
- Explicit false/nil preferences versus context true/non-nil: the actual save
  error returns unchanged and remains retryable without a time wrapper.

Binding assertions use independent table inputs and literal times. The owner
assertions inspect actual error policy and the SaveStep recorder's exact pending
and error identity; the pending=true/error answer is supplied only after binding.
No policy is implemented in test support. A later explicit RetryAt pointer edit
must not change the snapshot. The existing corpus cannot catch these wrong-source
regressions because it passes the same response on both paths. No new seam,
fixture, production setting or export is added.

The contract is deliberately narrowed rather than adding unused declarations or
schema-only checks. Removed fields and their actual owners are inventoried here:

| Excluded fact | Actual owner and scope |
| --- | --- |
| response UserError | `runInstance.ShouldRetry` reads `i.resp.UserError.NoRetry` for retryable error opcodes; ordinary StepRun does not consult it. The new tests set a conflicting context UserError and exercise ordinary StepRun only. No UserError source qualification is claimed. |
| response step limit | `responseRunner.run` uses `consts.DefaultMaxStepLimit` against response opcode count, not an executor setting. A single-op contract cannot exercise its threshold. Existing continuation corpus owns metadata-before-limit behavior; no new numeric-limit binding claim. |
| StartTime | `opcodeTiming` uses it as a fallback for trace start time. Trace timing input provenance is outside this persistence/discovery binding. The former snapshot field was unused by assertions and is removed. |
| ExecutionSpan / ParentSpan | RunContext tracing/request and other opcode effects retain these references. This increment does not qualify their source selection or span-reference cloning. Actual exported spans/carriers remain effect observations only. |
| lifecycle JobID | `discoveryBinding` derives a fresh job ID from metadata/opcode/coalescing rather than copying the incoming ID. Lifecycle listeners still receive the original live item; full lifecycle argument parity is outside this snapshot. |
| lifecycle EnqueuedAt / At | `opcodeTiming` reads these runtime times for tracing. They are `json:"-"`, excluded from this JSON projection, and are not portable trace timing inputs qualified here. |

P3 is addressed by narrowing the label and documenting exclusions, not retaining
new runtime fields. No new retained-field behavior is claimed. JSON-serializable
item fields are a representation; only the independently asserted priority,
max-attempts, edge and downstream identity inputs are qualified here. The original
live item still enters Go unchanged. Loaded metadata, metadata proposals, prepared
bytes and actual effect answers remain separate. This does not decide native
sequencing/ownership and does not close the parent architecture gates.


Final repair validation uses the same cached Go 1.26.4 binary, local toolchain,
GOMAXPROCS=2, cache temp directories, -p=2, -count=1 and 120s timeout documented
above. The identical focused selector (its `TestStepRunInputBinding` prefix now
also selects the new test) passed **144 test/subtest records in normal and 144 in
race**, zero failures/skips. All 139 prior passing names are present; the only
additions are the new parent and four cases. This is 288 combined pass records,
not 288 unique cases. No full suite/native/Rust/benchmark run is claimed.

Fresh logs and exact command arrays are under
`/home/ashutosh/.cache/step-run-input-binding/repair01/`:
`checks.json`, `normal.json`, `race.json`, `mutate.py`, `mutation-results.json`
and each named mutation log/overlay/source. The four original controls are
unchanged and each still fails seven subtests plus parent for its intended
assertion. Four added test-helper-only controls fail as follows (subtests only):
wrong-no-retry: 3; wrong-retry-at: 4; omitted-no-retry: 1; omitted-retry-at: 2.
These fail independent source-value/nil assertions, without panics, build failures
or an unrelated guard. `evidence.json` records log hashes, the baseline name-set
comparison and exact before/after source hash. Overlays leave checkout source
bytes unchanged; no restoration edits are necessary. The production owners were
never mutated, including in overlays. These controls qualify binding guards,
while the unmutated real-owner cases separately prove actual returned behavior.

Initial repair `preferences.json` exposed a test-author mistake: asserting
ErrorIs on NeverRetryError, which embeds the error without Unwrap. It is now
checked by message plus actual retry behavior; RetryAt preserves ErrorIs and the
default path preserves Same. An initial omission-control run exposed a nil
pointer dereference in the test; the final explicit NotNil assertion makes that
control fail cleanly for the missing fact. `preferences-final.json` records the
passing isolated test before that assertion strengthening; final normal/race and
all mutation logs include the strengthened assertion.

Final gofmt/whitespace and production/dependency/fixture diff checks pass.
The plan-status command was still known to exit 1 solely for the unrelated
status in `docs/plans/run-ai-debugging.md`; that file is untouched. Existing
work in the other test files is part of this accepted change. At this review01
handoff, the repair was uncommitted and independent review/parent acceptance
were pending; the later review02 decision and delivery state are recorded
below.

Final repair SHA-256:

```text
3814de806cbc75e4e3ad9ef4f5d6cc912c86f366a1781ccc12ee2da847dbd163  repair01/normal.json
32d7557909b6cc1dd07e3e9a4e5b6e632ab0a06c135c76929a2568d56928cc34  repair01/race.json
44f1b0b6e12915234f020a23d7f6f86ec744fdbd73cdc5895670c9312e456926  repair01/checks.json
3d4612eee9fda43b49b0cf866fc0397fc0e78d125c6ff3dd067caca1763250da  repair01/mutation-results.json
0ac001e8395ddf2b6f35a3c652a2934577ebceafc8668fee684ee34684915740  repair01/mutate.py
ab4c51dc72189661ef7f86ef392d8d7e590308b9499055266ed9fc40751bc2c9  repair01/evidence.json
156d36af7e2a109a9d5b973bed8c086c2f72f9120f857d5281c25857e4585b94  pkg/execution/executor/response_input_binding_test.go
```

## Independent review02 and parent acceptance (2026-10-27)

Independent Luna review02 found no blockers on the narrowed test-only scope.
Luna recorded 144 normal and 144 race test/subtest pass records, with zero
failures or skips. This is independent reviewer evidence, separate from the
author's 144 normal and 144 race records above. Luna inspected the eight
existing mutation logs but did not rerun the mutation cases; the mutation
results remain the author's recorded runs.

Parent acceptance is limited to an ordinary single non-AI StepRun at the real
`HandleGeneratorResponse` owner. The binding tests distinguish explicit response
`RequestVersion`, `NoRetry` and `RetryAt` from `i.resp`; the response preference
cases prove source selection through real SaveStep failures and returned retry
behavior. Detached loaded metadata remains separate from proposals, and the
actual SaveStep pending/error result is supplied only after binding. Queue item
coverage is a JSON projection: runtime `JobID`, `EnqueuedAt`, `At` and handles
are excluded. UserError, step-limit, start/span inputs and lifecycle runtime
fields remain unqualified. Copied JSON-serializable fields are not each
individually regression-proven. This evidence does not establish production
consumption, a production selector, native adapter, complete source coverage,
Go ownership transfer or native sequencing.

The original review01 P2 (source-preference regressions were not guarded) and
P3 (the lifecycle-item label overstated the snapshot) findings remain in the
history above. The four distinct-response owner cases and narrower JSON
projection description are the accepted bounded repairs. Author/reviewer
records and mutation logs are local evidence under
`/home/ashutosh/.cache/step-run-input-binding/repair01/`; mutation cases were not
rerun for review02.
