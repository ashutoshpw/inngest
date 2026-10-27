# Bounded enqueue-clock and bootstrap-accounting correction (2026-10-27)

**Current review state:** the parent records the bounded native clock semantics
and standalone precedence reviews as accepted within their declared scopes.
The P3 receipt below remains immutable evidence for its source snapshot. The
later readiness/comparator and pilot05 audit are recorded in
[one-shot readiness](response-owner-oneshot-readiness.md#current-scoped-review-handoff)
and the [consolidated pilot05 audit](response-owner-capability-pilot-05-audit.md).
Capture remains stopped because capability and lifecycle findings are open; no
full readiness, performance, or production verdict follows from scoped review
acceptance.

## P3 standalone eligibility precedence repair (2026-10-27)

**Implemented; scoped standalone-precedence review accepted.** Per the
parent's handoff, Sol semantics review02 accepted the reduced-profile staged
clock/bootstrap semantics and identified this standalone P3 ordering regression.
That earlier clock acceptance and the later standalone-precedence acceptance
remain scoped; neither establishes full capability or capture readiness. Both
capture entries remain **BLOCKED**. No delegation, commit, PR, pilot or benchmark.

Historical repro is preserved byte-for-byte at
`/home/ashutosh/.cache/oneshot-semantics-independent-4v5dz6hm/standalone-pending-future.json`.
For valid state, `hasPending=true, nowMs=1` historically returns `Ineligible`,
whereas the clock correction returned `InvalidConfiguration`; neither opens a
store connection. The standalone future-clock check and negative-clock
conversion had moved ahead of shared state validation and eligibility.

`EnqueueRequest::new` now performs its frozen-clock admission inside the clock
closure. Shared state validation, then eligibility, then discovery validation
still precede that closure. Historical future-run rejection remains for eligible
standalone requests. `with_clock` and the response owner's staged-clock code are
unchanged by this repair, preserving the accepted owner future-score branch.
There is no new production seam or changed descriptor/fixture.

The helper-boundary regression in `tests/helper_input.rs` protects observable
error precedence and zero store connections. Existing single-invalid admission
tests did not combine pending/unsaved/multiple-event eligibility with invalid
clocks. Nine cases include pending with future/valid/negative/zero clocks,
unsaved or multiple events with future clocks, pending plus invalid queue/state,
and an eligible future-clock rejection. Invalid state still precedes eligibility;
eligibility still precedes invalid queue/clock. The valid-clock pending control
isolates eligibility from the clock error. A live nonblocking listener detects
even a store connection before rejection.

New evidence is separate at
`/home/ashutosh/.cache/oneshot-p3-precedence-repair/`:

- `pre-fix-02` fails specifically on pending-future, receiving
  `InvalidConfiguration` instead of `Ineligible`. The first `pre-fix` attempt
  failed compilation because the test clock literal needed an i64 suffix; it
  executed no helper. The initial `fmt` failure and passing `fmt-final` are kept.
- `rust-tests-final`: **7 Rust tests pass**, one authenticated-store unit test
  intentionally ignored here and explicitly executed in the Go clock checks.
  Strict all-target Clippy, package Rustfmt and release helper build pass.
- `standalone-controls.json`: the same nine derived descriptors run against the
  historical accepted helper, pre-repair helper and repaired release helper.
  Historical/repaired each match **9/9**, including byte-identical stdout;
  pre-repair matches **3/9**, exposing six precedence differences. All **27
  processes open zero store connections**. The original repro is not rewritten.
- `standalone-normal` / `standalone-race`: unchanged exact fresh-cold fixtures in
  partition and key modes, unsharded `estate`/`queue`, **3 Go pass records each**.
- `clock-normal` / `clock-race`: all eight staged-clock cases, literal persisted
  fields, duplicates and pending suppression, **25 Go pass records each**.
  Each executes eight private native-test processes / fifteen native owner calls
  and fifteen reduced-Go calls. No skips, failures or race reports in these Go
  checks. Go race does not instrument Rust. No broad 78-record suite repeated.

Fresh release helper SHA-256:
`da132d4a292fd6a3645ec538d0971bba5b25dd04440ab72cd576cb9eea57126c`.
Fresh debug private clock-test executable SHA-256:
`c3c2ea1731a45746d0918acd096e442a626f5e4112899b0ffb231274da6f409f`.
Enqueue source SHA-256 changes from
`cef88b03952ec777ed41d8b1acb11025c7b21c66468b35faff9e9c624c7db0a3`
to `b255a1fc3e83d893eda1448c62c54bb61097e3d05a0bd0271d3412417696d050`.
`source-receipt-delta.json` explicitly records the only three differences from
the review01 source manifest: enqueue source, helper regression test and this
reference. All recorded Python files, exact fixtures and `response.rs` match
review01. Other existing uncommitted work is preserved.

The old review01 **global build receipt is historical, not current for this
source**. Its binaries, receipts and negative evidence remain untouched. The new
`build-receipt.json` is scoped to rebuilt native artifacts and these focused
checks; it is not a refreshed global capture receipt. It binds the new manifest,
build argv/optimization and artifact hashes. Exact command/log receipts, source
copies, isolated repair diff and evidence hash ledger are retained in the new
directory. HEAD remains `e6c3f82c0146ed37ef626d739c75fc85057885b7`.

The following sections retain earlier evidence and review states as history.

**Combined readiness review01 amendment:** final durable-evidence/range repairs,
current source/binary receipts and reconciled counts are recorded in
[one-shot readiness](response-owner-oneshot-readiness.md#readiness-review01-repairs-and-combined-evidence-2026-10-27).
The prior review's source drift was this separately authorized concurrent native
correction, not unapproved work. Its unchanged-owner premise does not apply to
the combined source. This document's original receipts and counts remain scoped
to the clock worker; its 8-method Python result predates the final 9-method /
6-synthetic-child suite. Fresh independent semantics/fairness and separate pilot
authorization remain required; both capture entries remain BLOCKED.

**Historical implementation snapshot.** At the current handoff, scoped clock,
standalone-precedence, comparator, and controls reviews are accepted as recorded
above. Both Python capture and Go pilot remain blocked by incomplete capability
and open resource/lifecycle findings. This history does not authorize capture,
pilot, benchmark, or performance conclusions.

Assigned checkout `/home/ashutosh/.t3/worktrees/inngest/t3-7411bca6`, branch
`t3/readiness-comparator-harness`, HEAD remains
`e6c3f82c0146ed37ef626d739c75fc85057885b7`. Preserve the readiness prefix and its
failures/hashes in `/home/ashutosh/.cache/oneshot-readiness/`; new evidence is
separate in `/home/ashutosh/.cache/oneshot-clock-correction/`. The previous
read-only Luna review, if delivered, reviews its prefix; it cannot accept this
later correction. No new review/delegation was requested by this work.

## Amendment and exact behavior

Original native-owner acceptance covered the single-explicit-clock reduced
domain, not parity with actual Go clock sampling. The original negative race
observation (discovery `1793101349462`, persisted wall `1793101349463`) remains
valid evidence of that limitation, not a data race or performance observation.
The parent separately authorizes these two bounded repairs. No broader
production, observer, lifecycle, recovery/fencing or language-advantage claim.

The unchanged Go sources inspected are `response_queue_effects.go`,
`queue/enqueue.go`, `queue/item.go`, `redis_state/queue.go` and the pinned Lua:

1. Discovery generates UUID and wall time after accepted save, including pending.
2. Producer `Score` samples its clock against **unadjusted discovery AtMS**.
   AtMS beyond producer time + 2 seconds retains discovery as FIFO score; exactly
   2 seconds still permits FIFO. Zero run timestamp also retains discovery.
   Otherwise score is run timestamp minus effective priority milliseconds.
   Item priority, including explicit zero, precedes identifier priority. Only
   after score selection does Go subtract priority from stored AtMS. Negative
   priority can put stored AtMS in the future without changing this score gate.
3. Redis enqueue samples a separate clock, always setting EnqueuedAt. If the
   passed FIFO time is before that clock, it overwrites WallTimeMS with enqueue
   time; otherwise it retains discovery WallTimeMS. FIFO is a millisecond time,
   but the comparison uses full clock precision. Equal persisted milliseconds
   can therefore still take the before branch.
4. Only that before branch samples the partition-pointer clock again; otherwise
   the pointer retains FIFO time. Partition pointers persist seconds. Key queues
   persist FIFO milliseconds in shadow indexes and ignore partitionTime in Lua.

Native `response.rs` keeps one invocation, generated UUID/time and conditional
enqueue ownership. A private clock closure samples at these sites, with full
precision durations for the before comparison. Pending takes discovery only;
failed save/preparation has no discovery binding. No clock, group, recipe or
completed effect is added to the public helper input. `enqueue.rs` retains the
standalone `EnqueueRequest::new` explicit-clock API, its original before-run
admission restriction and all original fixtures. The actual response owner uses
the private staged constructor; it follows Go's future-score branch rather than
rejecting a future run timestamp solely due to the old frozen-clock restriction.
No unrelated clock sites are refactored.

Comparator `step.saveRequest` now runs after successful output/validation and
before cold-client connection. Its real SwapMetadataSizeDelta consumes the
17-current/6-loaded claim even if bootstrap fails. A refused connection produces
counter 17 in each lane, no group/time/enqueue, a real Go bootstrap error with
null SaveStep result, and native NotSent/connect-or-auth with one attempt and no
reply. No Go sentinel or fabricated native accounting/ACK result is introduced.

## Test gate and independent proof

The regression contracts are literal persisted timestamps/indexes and consumer
latency, stage suppression on pending, preserving an existing duplicate item's
bytes, and accounting before failed connection. Existing frozen-clock fixtures
cannot detect advancing stages or cold-bootstrap ordering. The tests use actual
Go producer/shard clock options and a private Rust unit-test clock at the owner
boundary. There are no production Go test seams, sleeps, changed Lua or copied
planner answers. Original explicit-clock fixture tests remain independently run.

`TestResponseOneShotClockStages` prepares authenticated Valkey with the existing
Docker helper. For each lane it independently reads literal AtMS, WallTimeMS,
EnqueuedAt, FIFO/run indexes, all three partition pointers and durable accounting,
then decodes the item through Go's actual QueueItem and checks Latency and
ExpectedDelay. The native unit-test artifact receives the original descriptor
only through its private test stdin envelope; the real helper ABI is unchanged.
Eight cases cover advancing, negative priority, zero run timestamp, future
producer clock, exact two-second boundary, equal milliseconds, sub-millisecond
before and pending. Seven non-pending cases also repeat accepted save/enqueue
with different stage clocks and require the first stored item bytes to survive.

The requested advancing literal is asserted independently in both lanes:
discovery `1700000000990`, producer `1700000000995`, enqueue `1700000001005`,
pointer `1700000001009`, priority 19 seconds gives AtMS `1699999981990`, FIFO
`1469922831259`, WallTimeMS = EnqueuedAt = `1700000001005`, ready second
`1700000001`. None is derived from the native answer or a tolerance.

`TestResponseOneShotRefusedConnect` reserves a bound AF_INET socket **without
Listen**, preventing reuse while guaranteeing refused TCP connects. It exercises
both actual subprocesses, independently asserts their distinct errors and the
17 claim, and cannot send a store write. No fixed-port race or fabricated Go
adapter result. Existing functional/protocol/error-identity/fault/admission and
source-ownership controls remain selected.

The older native full-store reconstruction now binds observed generated
EnqueuedAt/pointer seconds to its Go oracle clock; its other effects are still
compared. Key-queue shadow FIFO scores are not misread as pointer seconds. This
reconstruction is not clock proof. One-shot non-clock comparison retains raw
readbacks, requires exact WallTimeMS = EnqueuedAt for its old-run fixed profile,
and bounds pointers by the actual invocation interval. The former arbitrary
100ms/one-second allowances are removed. Those bounds/normalization cannot prove
parity; fresh semantics and fairness blockers remain on both capture paths.

## Changed files and preserved scope

- `rust/crates/native-step-state/src/response.rs`: private owner clock and
  authenticated-store unit-test boundary.
- `rust/crates/native-step-state/src/enqueue.rs`: staged producer/enqueue/pointer
  bindings, unchanged standalone explicit-clock API.
- `pkg/execution/executor/response_oneshot_comparator_test.go`: actual accounting
  claim before bootstrap.
- `pkg/execution/executor/response_oneshot_readiness_test.go`: reserved-port
  regression, honest clock checks and unchanged capture refusal.
- `pkg/execution/executor/response_oneshot_clock_test.go`: new literal store oracle.
- `pkg/execution/state/redis_state/native_response_integration_test.go`: qualify
  the older generated-binding reconstruction.
- `scripts/response-owner-oneshot.py`: update the blocking explanation only for
  this correction; preserve all other readiness work in that file.
- This reference, native-owner/one-shot/measurement references, migration plan and
  status: amendment, evidence and acceptance history.

`scripts/test-response-owner-oneshot.py` was already uncommitted; additional
concurrent readiness edits to it and the Python runner were observed and
preserved. They are not authored by this clock/accounting correction. Their
current hashes are included in the final source manifest. The initial Python
failure and the later passing rerun are retained separately.

Production Go, Lua, original fixture bytes, helper descriptor/ABI, dependencies,
retry policy, write uncertainty, stdout/stderr bounds, admission, single actual
owner invocation, package budgets, memory accounting and no-performance gates
remain unchanged. The unrelated plan-status error is untouched. Only Linux
amd64 GNU is runtime-qualified here; Go race does not instrument Rust.

## Validation and provenance

Receipts in the separate evidence directory record exact argv/cwd, start/end,
exit and log SHA-256. Cached Go 1.26.4 uses GOTOOLCHAIN=local, cache
TMPDIR/GOTMPDIR, GOMAXPROCS=2 and -p=2. Rust 1.94 uses -j2; helper is --release,
private test executable is the default debug test profile. Docker is used only
through the authorized existing helper; unrelated containers are preserved.
Source manifests, source copies, immutable binaries, receipts and a hash ledger
are retained there. No performance package or resource capture was created.

Final validation uses `native-step-helper-sites` (SHA-256
`c7ddcbf957e25d448c27ef2e32508a93a012324bbbbaa8b7dc2c2f53f14e9475`)
and `native-clock-tests-sites` (SHA-256
`e14a0b7c465edabc2ef59dc62f1358d777b985b6f7518738975e02674f908ec2`).
The archived Go descriptor executable `executor-final.test` is SHA-256
`6d908c9e8504518e39859e0ccd62bf8e49963adfcc7fd9931b067a395c0bd12c`.
No earlier helper or binary is overwritten. The historical accepted helper hash
`18376b9f40071311e7bc6c37338862640e074f569533e9083fd33ab7a56fa39f`
remains historical evidence, not the corrected helper.

- Final `sites-normal` and `sites-race`: **99 Go test/subtest pass records each**,
  zero failures/skips/race reports. Select all five one-shot functional/protocol/
  error-identity/refused-connect/staged-clock tests and NativeRustResponseOwner.
- Final `replay-false-normal-sites` and `replay-true-race-sites`: **3 pass records
  each**, zero failures/skips/race reports, including normal and committed ACK
  loss in each selected queue mode. **204 final Go pass records total.** This is
  focused validation, not the full repository suite or a new recovery claim.
- Earlier broader normal/race selections ran twice, **268 passes per command**,
  including unchanged standalone explicit-clock fixtures, enqueue decode,
  admission and constructed-source controls. They precede the final relocation
  of the Redis clock sample after stored-ID hashing. Final core/replay checks
  above use the latest logical binding sites. Two additional fresh-only commands
  pass 3 records each after qualifying the older full-store reconstruction.
- `logical-sites-rust`: **six Rust tests pass**. One store-dependent private unit
  test is intentionally ignored by standalone cargo test and then explicitly
  executed, without skips, in each of the eight Go clock cases per lane run.
  `sites-fmt`, strict `sites-clippy` and release helper build pass.
- Latest Python `python-02`: **eight methods pass**, preserving current concurrent
  readiness repairs. Initial `python` failure retains the real package-deadline
  versus TimeoutExpired control failure; this correction did not overwrite that
  test or change its timeout classification. `final-declaration` still reports
  BLOCKED_FAIRNESS_REVIEW. No Python capture command was invoked.
- Git/new-file whitespace and Go format checks pass. Plan status exits 1 only
  for the known unrelated `run-ai-debugging.md` status, unchanged.

Final core/replay functional invocation counts: **164 helper processes**
(34 Go / 130 native), **16 private native-test processes** containing 30 explicit
native owner calls, and **38 in-process reduced-Go calls**. Repeated duplicate
calls are explicit test invocations, never adapter retries. These counts include
protocol/preparation rejects and are not observation or unique-case counts.

Across all author commands in this correction, including development, intended
controls, intermediate helper builds and reruns: **1,654 helper processes**
(104 Go / 1,550 native), **56 private native-test processes** containing 104
native calls, **127 in-process reduced-Go calls**, and **nine synthetic Python
children**. Existing broad-corpus helper counts are reconstructed from completed
reached table control flow, not a claim of PID tracing; one-shot child PID logs
remain available. Ordinary Go primitive-oracle calls and runtime replay internal
work are not counted as reduced response-owner invocations. The old readiness
prefix's separate 171 helpers/16 direct calls/7 synthetic children are not added
to this correction's totals or relabelled as new evidence.

Retained intended failures: `before-accounting` executes two children and fails
Go's counter assertion at 6 versus 17; native independently passes with NotSent.
`collapsed-clock-control-02` uses an archived temporary owner control that calls
the standalone frozen constructor at the corrected binding boundary: it fails
one native private invocation at 1 clock read versus 4. The saved control source,
binary and hashes are retained; corrected source was restored before functional
checks. This is an old-single-clock behavior control, not an untouched baseline
build. `clocks-development` validates seven clock cases but fails a new control's
incorrect Go JSON-null-as-empty assertion; the assertion is fixed to require
literal null, preserving the genuine bootstrap error. `collapsed-clock-control`
first failed due to wrong fixture cwd, with zero owner calls; its corrected cwd
run is retained separately. An initial evidence-runner `/dev/null.log` path error
occurred before any functional work and is not an owner outcome. Historical
`oneshot-readiness/final-race.log` remains untouched negative semantic evidence.

`validation-summary.json` lists every receipt and count; `build-receipt.json`,
`source-manifest.json`, `validated-source/` and `evidence-sha256.json` bind the
current source deltas/HEAD and immutable artifacts. Exact argv are retained in
`*.command.json`, including the final 120-second core test timeout and cached Go
normal/race commands. No thresholds or performance gates were evaluated.

Scoped independent semantics/fairness reviews have since been accepted within
their declared domains. Existing primitive instrumentation, startup, transport,
memory, and lifecycle limits remain blockers to broader claims. Review
acceptance does not authorize capture or complete readiness.
