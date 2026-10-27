# Fixed response-owner rejection screen

The later parent-authorized single screen completed on 2026-10-27:
**Parent-accepted SCREEN_NO_GO**, 18/18 rows and 9,594 validated slots in
90.037s. All three cells reject on latency and both rate gates; memory remains
INCONCLUSIVE. Independent Luna result review 01 recomputed the raw evidence and
confirmed byte-identical derivation, with no blockers; the parent accepted it.
See the [result and sealed evidence](response-owner-screen-results-2026-10-27.md).
The one-screen authorization is exhausted. Stop current FFI-boundary expansion;
the next architecture assessment is pending. Phase 1 remains partial, Phase 2
open, bridge unset, and production activation absent. The implementation history
and original authorization below are preserved as historical context.

Implementation-plan snapshot (before the accepted capture below): the parent
accepted this fixed-screen design, not a full-matrix retry. The implementation
task validated controls without running the screen; later controls acceptance
and the completed screen are summarized above.

Binding: `/home/ashutosh/.t3/worktrees/inngest/t3-a86a57bd`, branch
`t3/fixed-screen-mode`. The clean checkout had zero unique commits and was
fast-forwarded to `0cafa7475e94750da07a3485b78f9f763eef1b7b`, the
`t3/response-owner-measurement-harness` / [PR #13](https://github.com/ashutoshpw/inngest/pull/13)
base. The T3 binding and all previously sealed reports/bundles are preserved.

## Exact declaration and limits

New mode `screen` has fixed selectors, with changed/partial/duplicate selectors
rejected: seeds 26011/26012/26013; lanes `go,native`; profile `controlled`;
C1; GOMAXPROCS=8; workloads `run64,mixed8,run64k`. For each seed, cell order
and adjacent paired lane order are deterministically randomized before execution.
The complete declaration is written before any child runs. Every row has one
first request, 32 warmups, and 500 steady requests: 533 slots. Eighteen rows
contain 9,000 steady requests and 9,594 total slots. There are no separate
first-observation children. The existing functional preflight runs once, followed
by six one-request lane/workload port preflights; these six requests are separate
from the 9,594 row slots. All three seeds are mandatory, with no replacement.

Screen budgets are fixed and coordinated between Python and Go:

- 60s post-setup watchdog; 90s row context; 110s child test timeout;
  120s outer child kill. The per-request deadline remains 20s.
- A 1ms periodic observer, capped at 120,000 periodic resource records;
  the six lifecycle boundaries remain separate from that cap.
- One 900s monotonic wall budget from Python capture entry through source
  archiving, tool/version checks, all preflights, row children, provenance checks,
  derivation and final artifact hashing. Build and standalone review checks
  precede capture. **Ten seconds are reserved inside these 900s:** at 890s at
  the latest, measurement/setup/bookkeeping stops; up to 6s requests diagnostic
  shutdown (including up to 0.5s kill/reap), leaving 4s for failure sealing. The Go parent receives only the
  remaining active budget. Its test timeout allows the 6s control grace within
  the original 900s, rather than panicking at the active deadline before flush.
  Each row's unchanged 120s outer bound includes a 5s command shutdown reserve
  (cooperative cancellation at 115s, then hard kill by 120s). Functional preflight
  cancellation similarly reserves 5s inside its existing 90s bound. No automatic
  extension, row retry or matrix continuation.
- Source paths/stat signatures are checked while the child runs (at most once
  per second), followed by full source/binary/native hashes. A detected change
  terminates capture. These host-side checks are observer overhead, not a claim
  of isolation or CPU usage. The 1ms in-row resource observer is unchanged.

A first mismatch, request/row/package deadline, functional failure, sampler cap,
or detected provenance change stops capture. Package/provenance STOP sends
SIGUSR1 to the measurement process group. The Go parent cancels further child
launches; its active command requests SIGUSR1 and waits at most 5s before kill.
The row registers SIGUSR1 and starts its failure coordinator before port setup.
One mutex protects request admission, publication of scalar request/input state,
STOP snapshot ownership, and the normal final-write handoff. Every first, warmup
and steady request passes this gate before input preparation. Admission checks
both the closed gate and cancellation already observable in the row context; it
publishes the slot start and `preparation` state before releasing the gate.
Admission covers that whole operation: input preparation, owner dispatch,
validation and release. An operation admitted before STOP can continue through
those steps; no later operation can begin preparation or dispatch. This is an
observable admission/STOP linearization, **not** the instant an external signal
arrives or the instant context cancellation occurs outside the mutex.

The coordinator closes admission and copies published immutable samples, start
markers, input counts and locked resource records under the same gate. A slot
admitted but still preparing (including a returned input not yet published under
the gate) is explicit as `preparation_at_cooperative_stop`, never `not_started`.
Published retained inputs are `prepared_at_cooperative_stop` with corresponding
input counts; owner-started slots remain in-flight and closed samples remain
recorded. Counts describe published roots at that snapshot epoch, not allocations
that admitted preparation might publish later. A denied slot has no slot/owner
start. No lock spans input creation, owner/port I/O, validation, native calls or
callback drain. Snapshot copying adds control overhead; it supplies no performance
evidence. The existing owner/slot clocks, baseline/ready/heap observations and
large-buffer allocation positions are retained.

The row writes its owned snapshot before bounded Go/native stacks, then exits
unsuccessfully without a native join, input/callback reclamation or scheduler
close. The row reason is `cooperative_stop`; the precise outer reason stays in
the command/manifest. The coordinator can snapshot planned slots even during
setup. If setup returns after cancellation before the observer runs, finalization
waits for that coordinator rather than overwriting its diagnostic. A separate
row-write mutex serializes the initial setup write before the failure replacement;
a late initial write cannot overwrite a claimed failure snapshot. It never locks
request owners or extends shutdown grace. The watchdog's timer still starts only
after setup/initial write. A process killed before signal registration, stalled
I/O/runtime, hard kill or host failure may leave only earlier STARTED evidence.
There is no periodic checkpoint or per-request I/O in ordinary measurement runs;
the 1ms observer, budgets and measurement time boundaries remain unchanged.

Each functional/port/row child has a checked STARTED command ledger before launch,
a live streamed stdout/stderr log, and a final COMPLETE or INCOMPLETE ledger after
wait. Go checks copying, sync, close, initial row, final row, stack and command
write errors. JSON replacements use checked temporary writes and rename so a
failed replacement retains the previous complete record. All Python artifact
writes/sync/renames are checked too. Python streams the screen parent's output
through a nonblocking pipe in bounded 64KiB chunks during the existing 0.25s
control poll and drains it on stop. This makes actual run.log write errors fatal;
passing a log FD straight to a child cannot report that child's failed writes.
The parent produces control output, not per-request output; no disk operation is
added to owner request timing. New captures declare mandatory child evidence;
`screen` always requires it. Derivation rejects missing/unreadable logs, missing
or STARTED/failed command records for every functional/port/row child. Existing
sealed non-screen bundles without this declaration retain their historical
interpretation; none is rewritten or promoted to this stronger capture guarantee.

`CAPTURE_INCOMPLETE` is created before capture work and removed only after final
writes/checks succeed. It is a transient control marker, excluded from the artifact
hash ledger; its presence blocks external derivation even when a persistent write
failure prevents a STOP manifest/failure seal. Failure invalidates any derived
file first, retaining it as `unaccepted-derived.json`. A failed final command
replacement leaves STARTED evidence; a hard-killed outer controller can similarly
leave a missing final exit, always an incomplete attempt. Failure writes remain
fatal and visible on stderr; filesystem/OS failure can prevent the seal itself.

Python allows at most the remaining control grace for cooperation, then kills
the group, including descendants of an already exited Go parent. The original
hard package deadline also bounds cleanup/sealing; exhausted control time reports
`diagnostic flush/seal unconfirmed; no verdict`. Native FFI, an unresponsive Go
runtime, slow/unavailable disk, unavailable gdb/ptrace/symbols, SIGKILL, host loss
or an OS failure can prevent any additional evidence. Previously persisted bytes
and streamed output are retained where the filesystem permits; preservation of
in-memory completed observations is **best effort**, not a SIGKILL durability
promise. A native hang cannot guarantee graceful full join. The row is flushed
before best-effort native stacks (3s tool context + 1s pipe close bound; 2MiB),
so inability to unwind native work does not require waiting to save known data.

No incomplete/provenance-changed/write-failed attempt yields a verdict. Success's
manifest elapsed value is the snapshot before derivation; the active alarm stays
armed through reducer and final hashing. Sparse RSS alone is memory INCONCLUSIVE
and permits valid latency/rate analysis. No outlier, timeout or contaminated row
is removed or replaced.

The full `plan`/`campaign`, `smoke`, and one-row `diagnostic` declarations and
budgets retain their existing meanings and defaults. `screen --declare-only`
executes no functional/port preflight or row, and cannot be reduced as a screen.
`plan` still declares the full campaign; it is not a screen alias. Ordinary Go
tests still skip measurement without explicit mode selection.

## Decisions and unchanged measurements

Raw schema 2 and derived schema 3 remain. Resource p95 is nearest rank over
`resource_ns-start_ns`; closed-loop achieved rate includes per-request input
preparation, independent validation/release and between-call gaps. Owner service
rate is inverse mean **wall occupancy**, `N/sum(resource_ns-start_ns)`, at matched
fixed C; it is not CPU consumption. Actual native projection, copies, codecs,
allocation and drain stay timed. Neither JSON nor owner workload semantics change.

Thresholds remain UNCALIBRATED: p95 <=1.10, closed-loop rate >=0.90,
owner service rate >=0.90, whole-host peak RSS <=1.20. Each metric needs a passing
median paired ratio and at least two of the three seeds. Input retention,
all six strict lifecycle boundaries, RSS peak window and sampling density guards
remain in force. There is no precise Go-versus-Rust memory attribution.

`cells[].status` and `cells[].memory` preserve the existing aggregate and memory
statuses. New `cells[].screen_decision` is independent: a completed three-seed
cell with a failed latency, closed-loop rate, service rate or conclusive memory
gate is `NO_GO`, even if the aggregate is memory INCONCLUSIVE. Passing gates
produce `PROMISING_NEEDS_BROADER_GATES`; otherwise the screen is INCONCLUSIVE.
The top-level decision uses the same rejection-first rule, with statuses
`SCREEN_NO_GO`, `SCREEN_PROMISING_NEEDS_BROADER_GATES`, or
`SCREEN_INCONCLUSIVE`. No overall screen PASS, transport choice or activation is
possible. Missing/incomplete seeds/rows, timeout, bad phases, invalid retained
inputs, missing lifecycle completion, failed preflight or package expiry yields
STOP before any per-cell verdict is published.

C8, Valkey and the retryable/enqueue-error workloads remain UNMEASURED. The
persistent-process comparison, native arm64/final Cgo link, checkpoint, late
CritT, Stop, fencing, uncertain/lost acknowledgement, lifecycle/recovery,
calibration and activation gates remain open. Phase 1 is partial; Phase 2 open;
bridge unset.

## Authoring gate and control checks

The Go declaration/control tests own exact scope/counts, deterministic paired
seed order, six one-request preflights and fixed child budgets. A scope expansion,
missing seed, extra first child or child launched with the old budgets violates
that contract; the old full-campaign selector tests alone cannot catch it.
Existing campaign/diagnostic and timing-boundary tests remain retained.

Python synthetic scalar fixtures exercise the real reducer for separate latency,
closed-loop and service rejections with sparse memory, median/2-of-3 seeds,
missing/duplicate seeds, incomplete/contaminated rows, preflight failure,
sampler/budget/provenance corruption, and no overall PASS. Mocked child execution
with a simulated clock exercises the actual capture controller: setup consumes
remaining budget, package expiry terminates the active group and seals failure,
provenance change stops it, and successful child exit cannot bypass a later
bookkeeping deadline. These mocks execute no owner workload and provide no
performance evidence. They do not establish partial preservation; the real-process
regressions below own that contract. No production seam was introduced.

Validation evidence is outside the checkout at
`/home/ashutosh/.cache/response-owner-screen-controls-20261027`.
Exact review commands/results and final checks are recorded below after validation.

## Ready commands and acceptance gate

Run from this bound checkout. Cached Go 1.26.4 and cache-backed temporary space
are required; compile with GOMAXPROCS=2 and `-p=2`, execute controls with
GOMAXPROCS=8. The cached Rust archive was copied only after identical tracked
Rust sources/Cargo.lock and accepted SHA-256
`562139fab7fec3ed44cdcb596276c1241d038ed95c0d34a165fa7d6d0f28ed7d`
were verified. No Rust rebuild was needed. A different host/checkout needs the
native build prerequisite in [real effects](response-real-effects.md).

```sh
# Build/review controls; these commands do not run the screen.
SCREEN_REVIEW_DIR=/home/ashutosh/.cache/response-owner-stop-evidence-20261027
mkdir -p "$SCREEN_REVIEW_DIR/tmp"
export GOTOOLCHAIN=local
export GOTMPDIR="$SCREEN_REVIEW_DIR/tmp" TMPDIR="$SCREEN_REVIEW_DIR/tmp"
GO=/home/ashutosh/.cache/go1.26.4/go/bin/go
GOMAXPROCS=2 "$GO" test -p=2 -c -tags=response_continuation_spike \
  -o "$SCREEN_REVIEW_DIR/executor.test" ./pkg/execution/executor
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-response-owner-measure.py -v
GOMAXPROCS=8 "$SCREEN_REVIEW_DIR/executor.test" \
  '-test.run=^TestResponseMeasure(CaptureControl|Selectors|CompletionBoundaries)$' \
  -test.v -test.timeout=90s

# New destination required; declaration only, no preflight or owner rows.
GOMAXPROCS=8 "$SCREEN_REVIEW_DIR/executor.test" \
  '-test.run=^TestResponseContinuationMeasure$' \
  -response-measure-mode=screen -response-measure-declare-only \
  -response-measure-out="$SCREEN_REVIEW_DIR/declaration-next" -test.timeout=30s
```

After independent Luna review and **explicit parent acceptance to execute one
screen**, confirm the binary/source/native hashes and quiet/stable source tree,
unset inherited private child markers, and use a new external bundle. No Valkey
server/container is required for this controlled screen. The capture itself
retains the existing functional suite and all six port preflights; do not omit
or substitute them. The earlier 110s campaign STOP and 156s diagnostic establish
budget context only; the roughly 16.5s estimate for 500 payload samples is also
budget-only, never a performance prediction or reason to extend the package.

```sh
# FUTURE ONLY — not executed in this implementation task.
unset RESPONSE_MEASURE_CHILD RESPONSE_MEASURE_ROW_PATH RESPONSE_MEASURE_VALKEY_ADDR
unset RESPONSE_MEASURE_COMBINED_CONTROL RESPONSE_MEASURE_CONTROL_BARRIER
unset RESPONSE_MEASURE_CONTROL_HELPER RESPONSE_MEASURE_CONTROL_PATH
GOMAXPROCS=8 PYTHONDONTWRITEBYTECODE=1 \
python3 scripts/response-owner-measure.py capture --mode screen \
  --binary "$SCREEN_REVIEW_DIR/executor.test" \
  --out "$SCREEN_REVIEW_DIR/screen-parent-accepted-once" \
  --lanes go,native --profiles controlled \
  --workloads run64,mixed8,run64k --concurrency 1
# Raw bundle is immutable; choose a new output for independent reproduction.
python3 scripts/response-owner-measure.py derive \
  "$SCREEN_REVIEW_DIR/screen-parent-accepted-once" \
  --output "$SCREEN_REVIEW_DIR/screen-rederived-v3.json"
```

Stop after this one screen, including any failure; preserve the full bundle and
request a new parent decision. Never expand to the full matrix automatically.
The [original campaign STOP](response-owner-measurement-campaign-2026-10-27.md),
[accepted diagnostic](response-owner-diagnostic-2026-10-27.md), their structured
reports, and every sealed raw artifact remain unchanged.

## Implementation validation and review handoff (pre-blocker snapshot)

- Fresh tagged normal build and tagged race build passed using cached Go 1.26.4,
  GOMAXPROCS=2, `-p=2`, and the cache-backed `tmp/` directory. The normal binary
  SHA-256 is `38aae060e4a544f924c04011a22b2fdbec56ec7e333eecba728d52ddcc0ef6d6`.
  No production/driver/ports/workload/Rust/ABI/model/fixture source changed.
- Focused normal and race selections each passed 17 top-level tests with no
  skips: the existing 14-test functional preflight, including both 40-literal
  suites and lifetime/poison/admission witnesses, plus selectors, capture controls
  and completion boundaries. Normal: 0.729s outer command; race: 3.688s outer
  command. These are test execution times, not measurement results. Exact argv,
  environment, UTC times, status and logs: `focused-normal-command.json`,
  `focused-normal.log`, `focused-race-command.json`, `focused-race.log`.
- Final Python check passed 11 methods (4.437s unittest time). It includes all
  existing memory/retention/strict-boundary/service/diagnostic checks and the
  screen synthetic/mocked cases. The final mock also verifies failure sealing
  when the package deadline interrupts a manifest write. Exact command/result:
  `python-controls-command.json`, `python-controls.log`.
- Direct Go declaration-only check passed, then Python's actual declaration-only
  CLI passed source archiving, version checks, remaining-budget propagation and
  final ledger validation. Go's `declaration/` and Python's
  `screen-declaration/rows/` each contain only `selection.json`: 18 rows,
  9,000 steady/9,594 total row slots, zero executed measurement/port/functional
  children and no derived result. Python passed 880.085165162s remaining after
  setup into Go. Its six-file ledger verifies, with no source/binary/native
  change. This was a control/declaration check, not an 18-row capture. The sealed
  snapshot predates only the final Python interrupted-manifest preservation guard
  and review-result prose; final Python controls cover that guard. Do not rewrite
  this declaration bundle. Exact argv/results: `declaration-command.json`,
  `python-declaration-command.json`; raw declaration bundle `screen-declaration/`.
- `git diff --check` passes. `scripts/check-plan-status.sh` exits 1 for the
  pre-existing invalid `Implemented — pending staging review` status in
  `docs/plans/run-ai-debugging.md`; that unrelated file is unchanged.

Changed files for review: `response_continuation_measure_test.go`,
`scripts/response-owner-measure.py`, `scripts/test-response-owner-measure.py`, and
this task's existing `plan.md`, `status.md`, `response-owner-measurement.md`, plus
this new reference. All changes remain uncommitted on the binding/base recorded
above. PR #13 is linked for context, with no PR update or new PR. No Luna review,
screen capture, campaign, commit, push or delegation was performed. Stop here for
independent Luna review; parent execution acceptance is still required.

## Accepted capture-evidence blocker repair (2026-10-27)

Prior Luna finding: Python immediately killed the Go group on expiry/provenance
change; the row's completed samples existed only in memory until its own watchdog.
Go CombinedOutput buffered child stdout/stderr until return, then ignored row/log/
command write errors. Reduction required row JSON but not child command/log
receipts. The old mocked STARTED/failure-seal check did not prove preservation.
All other screen controls were accepted and remain unchanged.

The bounded STOP protocol above repairs only that finding. Authoring gate:
real signals/processes own preservation and escalation; temp-filesystem failures
own durable-write admission; the actual reducer owns evidence eligibility. A
return-only mock, duplicated functional fixture, per-request I/O hook or new
production export cannot test these risks. Retained existing controls cover exact
scope, budgets, thresholds, phases and lifecycle boundaries independently.

- Python real-process regression runs actual SIGALRM expiry and actual provenance
  detection. Its bounded helper has four completed in-memory records and stdout;
  only SIGUSR1 flushes them. Both stops retain all four, both output messages,
  final INCOMPLETE command/exit, STOP manifest, hash ledger and failure marker;
  actual derivation refuses both. The pre-fix controller failed both cases on
  missing completed records (`python-prefix.log`), rather than a fake-clock or
  missing-tool error. This helper is capture control, not owner performance.
- Go real-process controls stop an actual native/controlled/run64k row after a
  short completion interval (2s to allow instrumented race execution too).
  Multiple closed samples, resource records, available
  Go stacks and streamed stdout survive; unfinished slots and final INCOMPLETE
  command evidence remain. A separate child ignores SIGUSR1: real context
  cancellation escalates at the 5s grace and records flush-unconfirmed, never
  success. No whole row/screen/campaign is completed for this control.
- Filesystem controls fail initial row/command writes, log open, actual streamed
  log writes (a child-process-only RLIMIT_FSIZE on a temp file, plus Python's
  temp bundle log pointing to /dev/full), final command
  replacement, diagnostic row replacement, initial/final Python manifests and
  artifact ledger writes. Assertions require the intended file/error. Failed
  atomic replacements retain previous records; persistent seal failure retains
  CAPTURE_INCOMPLETE and external derivation rejects it for that reason.
- A success-shaped complete three-seed fixture rejects missing command, missing
  log, or missing final update independently for functional preflight, port
  preflight and selected row. Existing successful reducer controls retain their
  same numerical results. No historical sealed bundle or report was modified.

Evidence root: `/home/ashutosh/.cache/response-owner-stop-evidence-20261027`.
Final command metadata, validation counts and limits follow below. This repair
remains uncommitted for fresh Luna review; no review was delegated here.

### Final repair validation and handoff

- Cached Go 1.26.4 tagged normal/race builds pass with GOMAXPROCS=2, `-p=2`,
  cache-backed GOTMPDIR/TMPDIR. `build-normal-command.json` / `build-race-command.json`
  record the exact commands, environment, UTC bounds and exit status.
- The retained 14 functional tests plus selectors, completion boundaries and
  capture controls pass all **17 top-level tests without skips** in normal and
  race binaries: 11.643s and 19.219s outer command times, respectively. These
  times describe control tests, not performance. `focused-normal-command.json`,
  `focused-normal.log`, `focused-race-command.json`, `focused-race.log` contain
  complete evidence. The actual owner/slot timing functions `measureRequest` and
  `measureDeadlineContext` are byte-identical to base HEAD.
- Final Python run passes **14 methods** (7.617s unittest time, 7.695s outer
  command). Exact command/environment/results: `python-controls-command.json`,
  `python-controls.log`. Both real signal stops retain the helper's four records
  and output. Filesystem errors are asserted for their intended reason, including
  actual ENOSPC on the Python output stream. No capture or derivation of an
  18-row workload bundle was run.
- The first race attempt exposed insufficient allowance for the native test's
  instrumented completions and Go Wait preferring a child's SIGPIPE over the
  output-copy EFBIG. The test now permits a 2s completion interval; the writer
  independently retains the filesystem error even when child exit masks it.
  `focused-race-before-log-error-retention.log` and matching command metadata
  preserve that finding; the final normal/race runs above pass. Ordinary failure
  returns also persist row data before attempting native stacks.
- `final-review-manifest.json` records source/binary/log/command hashes and the
  exact unchanged base/branch. Normal binary SHA-256:
  `9fa7f800c3e4ec576515758115599ae536f31fac626ff58c76d4423eaf0a7d7d`;
  race binary: `878992ef5bb05d59e6d24dbeb2ac1d356cdfc07deac7033255179c5107345261`.
- `git diff --check` passes. The plan-status script still exits 1 solely for the
  pre-existing invalid `Implemented — pending staging review` status in
  `docs/plans/run-ai-debugging.md`; `plan-status-command.json` and `plan-status.log`
  record it. No unrelated plan was changed.

Stop uncommitted here for fresh Luna review. No delegation, commit, push, PR
operation, screen/campaign, new performance result, source ABI/driver/fixture edit,
threshold/default workload change, or modification of an old sealed bundle was
performed. The bounded controls use real native work only as short forced-stop
checks, not a completed measurement row or comparative result. Phase 1 remains
partial, Phase 2 open, bridge unset; parent execution acceptance remains required.

## STOP admission linearization repair (2026-10-27)

Accepted Luna review 02 identified the remaining P2: `run` checked only a later
asynchronous `stopWork`, so cancellation during setup could still prepare and
execute the first request, or lose its admission from a `not_started` snapshot.
The gate/coordinator protocol above replaces that independent atomic check.
Only the tagged measurement test file, its Python controls and task documentation
changed in this follow-up. The Python capture controller is byte-identical to
the preceding accepted capture-evidence repair. Production, ports, driver, Rust,
model, fixture, threshold and default budget/workload sources are unchanged.

### Test authoring gate and real combined path

The independent contract is request admission and snapshot ownership across a
real process-group STOP. The credible regression is preparation after observed
cancellation, or admitted/prepared work reported as not started. Existing Python
helpers synthesize row records; existing Go controls signal a row directly.
Neither exercises Python -> real Go parent -> real command runner -> native row.
The new `StopEvidence.test_real_python_go_parent_row_stop` owns that combined
transport/lifecycle gap. No production seam was added: the configuration and
barriers exist only in the Linux/Cgo/tagged measurement `_test.go` harness.

Python's real capture controller launches an exec-only selector wrapper into
`TestResponseMeasureCombinedControl`, which calls the existing real parent entry.
The parent executes the unchanged functional preflight and one real native port
preflight, writes its real selection/STARTED/final command records and launches
`TestResponseContinuationMeasure` as its row child. Explicit helper configuration
declares **five** run64/native/controlled/C1 slots (first + one warmup + three
steady), never a 500-request row or 18-row screen. The original direct native
signal control now shares this tiny configuration and uses the published barrier
instead of a completion-delay guess or a 5,000-request row.

Four deterministic barriers stop during setup/pre-first, admitted preparation,
published prepared input, and after two fully published first/warmup completions.
Setup delays the observer until the first admission attempt, ensuring that a
cancelled context must prevent work even before that observer is scheduled.
Other barriers park the admitted operation so the failure snapshot can be checked.
Only the outer STOP trigger (`remaining` raises once the barrier file exists),
one-source metadata selection and test selector are controlled; no parent,
command runner, row, owner work, signal delivery, stream, ledger or failure seal
is faked. The local 30s test package bound uses the existing control/seal reserves;
actual screen budgets are unchanged. Each successful combined test checks:

- Zero setup admissions and no preparation log after cancellation; exactly one
  admitted preparation/prepared slot, or exactly two closed samples after the
  publication barrier, with remaining slots not started.
- Correct retained-input and partial states, streamed parent/row output, COMPLETE
  functional/port receipts and final INCOMPLETE row/outer command records.
- STOP manifest, incomplete marker, artifact ledger and actual reducer refusal;
  both row/parent PIDs and their process group are gone after cleanup.

The preparation barrier models unpublished preparation; it does not claim to
interrupt native FFI or validate every OS scheduler interleaving. Published roots
and unfinished operations remain tied to the observable gate epoch. The retained
unresponsive-child control independently proves bounded hard-kill escalation;
it does not establish that a specific native call can be cancelled. Repeated
SIGUSR1 is retained by signal notification while diagnostics run. Parent-first
exit still uses Python's bounded descendant/group cleanup; there is no full-join
or hard-kill durability claim. Artifact failures remain fatal, and no partial
attempt receives a verdict.

### Reproduction, final checks and evidence

New evidence root (older sealed bundles remain untouched):
`/home/ashutosh/.cache/response-owner-admission-controls-20261027`.

- `pre-fix-with-controls.go` contains the preceding owner/failure projection with
  only the new test harness. `build-prefix-command.json` builds
  `executor-prefix.test`. `combined-prefix-command.json` and `combined-prefix.log`
  prove the regression fails for the intended reason: setup output contains
  `control input preparation 0` after cancellation; preparation/prepared cases
  also lose their admitted slot (0 versus 1). The published case passes.
  Full partials/streams/receipts/seals are in `pre-fix-stable-bundles/`.
- One earlier prefix attempt was invalidated by restoring source during capture;
  `combined-prefix-source-change.log` and its command metadata preserve that
  provenance failure. It is not the regression proof. Subsequent capture controls
  ran with stable source. Earlier exploratory bundles are preserved separately.
- Final cached Go 1.26.4 normal/race builds pass with GOMAXPROCS=2, `-p=2`,
  cache-backed GOTMPDIR/TMPDIR. Exact argv/env/cwd/UTC bounds/exit status are in
  `build-normal-command.json` and `build-race-command.json`.
- The retained **17 top-level Go tests, no skips**, pass in both binaries from
  `pkg/execution/executor` (including all 14 functional preflight tests):
  `focused-normal-command.json` / `.log`, `focused-race-command.json` / `.log`.
  Outer test times are 6.977s and 13.282s, never performance measurements.
- Python passes **15 methods, no skips**, with each binary: the preceding 14 plus
  the new four-case combined method. `python-controls-command.json` / `.log`
  (13.881s outer) and `python-race-controls-command.json` / `.log` (32.739s outer)
  record them. `final-normal-bundles/` and `final-race-bundles/` preserve all eight
  real combined partial bundles. The actual owner and deadline-context timing
  functions remain byte-identical to base HEAD.

To repeat controls only, use a new external evidence directory, build normal and
race binaries as in the ready build commands, then run the focused Go expression
recorded in those command receipts from `pkg/execution/executor`. For Python:

```sh
RESPONSE_MEASURE_CONTROL_BINARY="$NEW_CONTROL_DIR/executor.test" \
RESPONSE_MEASURE_CONTROL_GO=/home/ashutosh/.cache/go1.26.4/go/bin/go \
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-response-owner-measure.py -v
# Repeat with executor-race.test; optional RESPONSE_MEASURE_CONTROL_EVIDENCE
# retains copied control bundles in a new, empty destination.
```

Final hashes, retained-evidence verification and whitespace/format/status checks
are in `final-review-manifest.json`. Stop here uncommitted for fresh independent
review. No delegation, commit, push, PR operation, screen/campaign or performance
conclusion is part of this follow-up. Broad Phase 1 remains partial, Phase 2 open,
and bridge unset.

Final integrity checks pass: gofmt, Python AST and `git diff --check`; no surviving
control binary process was found. All eight new control artifact ledgers verify
(17 files each). Historical campaign/plan/diagnostic ledgers verify all 101/6/8
entries, and the preceding repair's two binaries and 18 evidence files retain
their recorded hashes. The native archive retains accepted SHA-256
`562139fab7fec3ed44cdcb596276c1241d038ed95c0d34a165fa7d6d0f28ed7d`.
Final normal binary: `9d1cd95f1dbbe06d79dff2940f3b19b0558dcc41bb0c734d3858c1c2118812a4`;
race binary: `bd52b16cc5167157171acd67e3fc5763308afe0dd4214ed2ba1dc3e7314ebc9a`.
The plan-status script still exits 1 solely for the pre-existing invalid status
in `docs/plans/run-ai-debugging.md`; its exact receipt/log are retained. That
unrelated plan is unchanged. Final review manifest hashes bind this documentation,
the implementation, binaries and evidence on the original branch/base.
