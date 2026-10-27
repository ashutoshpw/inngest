# Matched response-owner measurement harness

Updated 2026-10-27. Parent accepted the matched harness and the limited one-row diagnostic evidence after the matched-port/timing/raw-reproduction, bounded-input-retention, service-rate, RSS-boundary, strict-enclosure and final diagnostic reviews. This accepts harness and diagnostic integrity only; the measurement decision remains pending, with no performance verdict or additional campaign authorization.
Base f9cf1892c0de4e1c9a6cedd8cbcfe8e2473e5ae0; isolated T3 worktree
`t3/response-owner-measurement-harness`. Clean/no unique commits verified before fast-forward.

Harness only: no production changes, Rust ABI/model changes, fixture answers,
activation, full campaign, performance claims or bridge selection. Phase 1 stays
partial, Phase 2 open, bridge unset. Persistent-process comparison remains required
before selection. Native arm64/final Cgo link, checkpoint, late CritT, Stop, fencing
and uncertain/lost acknowledgement gates remain unchanged.

## Test authoring gate

Retain all 40-literal parity and lifetime/poison/admission witnesses during factoring.
New harness regressions protect independent contracts: delayed callbacks must charge
delivery/resource latency; delayed cleanup must charge slot reuse; invalid/missing
selectors must fail before running. Existing functional fixtures do not observe
measurement timestamps or row selection. No production seams are added.

## Implemented boundary and ports

The experimental host is factored, without behavioral edits, into
`response_continuation_driver_spike_test.go`. Admission mutex, cancellation
watcher, consumed Resume handling, poison/no fallback, outstanding-effect joins,
callback retention and Close/drain remain intact. Existing tests and fault
barriers stay in the integration file; `beforeAdmission` is nil in measurement.
No timed helper depends on testing, require, fixture answers or synctest.

`response_continuation_measure_ports_test.go` constructs inputs with ordinary
error returns. Both lanes use actual descriptors (Timing, Userland, options and
metadata), real clocks, recording incoming spans, production tracing with a
bounded non-retaining exporter, one sync and one async listener, priority,
concurrency keys, semaphores and metadata delta contexts. Context values and
loaded metadata snapshots survive. Span/carrier construction and metadata
processing execute in the real Go effects. There is no external telemetry or
capacity-lease claim.

- `controlled`: substitutes only state and queue I/O. Full output/queue bytes are
  consumed with SHA-256; at most eight output digests and pending IDs are retained
  per session. Pending/error observations come from the state port. The actual
  Go conditional handlers, tracing, preparation and queue materialization run.
- `real-valkey-off` and `real-valkey-on`: production redis_state V2 and producer/
  shard construction equivalent to the existing storage witness, without its
  recorders. Each child uses two equal clients, fresh namespaces and independent
  precreated runs. Create is followed by production metadata update/increment
  and independent readback so storage really matches the loaded config/7-byte
  metadata snapshot. The local current size is 11 bytes. Independent output,
  pending, metadata, queue identity and carrier readback follows timing.
  No consumer runs; no end-to-end queue throughput is measured.

The existing owners copy the RunMetrics value into every branch. Thus each
saved branch in these inputs sends 4 previously unpersisted bytes plus 4 bytes
of emitted metadata, including 40 total bytes for the five saves in mixed8.
The harness preserves this existing behavior; it does not repair or endorse
broader shared-accounting semantics. Both lanes are checked against the same
literal delta and actual persisted baseline.

One shared monotonic start precedes the actual Go entry or native constructor.
`owner_return_ns` follows final joins/disposition. `delivered_ns` follows every
actually launched callback; sync scheduled notifications account for launches
before owner return, even if the async callback has not started. The native
wrapper callbacks are additionally joined. `resource_ns` follows native drain/
Close and binding release, incoming span end and request cancellation. Go charges
its appropriate span/context cleanup. Resource completion is primary, and slots
reuse only after it. There is no sibling save barrier. Native generation,
projection, branch copies, codecs, FFI/locks/clone/validation, C.GoBytes/release,
key checks, effect goroutines, recipes, original errors/wrappers and close remain
inside timing. Go incurs no Rust projection.

## Capability, not an executed campaign

The full `plan`/`campaign` selectors declare three seeds 26011/26012/26013,
GOMAXPROCS=8, both lanes, C1/C8, five workloads and three profiles: 180 steady
rows/360,000 responses. Controlled rows have 5,000 steady requests; Valkey rows
500. Each row is a fresh compiled-test process with one first request, then 32
warmups per concurrency slot. Native creates/closes every session. Ten separate
fresh-process first-observation pairs per workload/profile at C1 add 300 children.
Paired lane order and cell order are seeded/randomized. These are fresh-process
first observations, not cold-cache measurements. Launch/setup are separate.

Workloads are StepRun with exactly 64-byte saved valid JSON; mixed8 containing
all six eligible tags (StepError permanent); StepRun with exactly 64-KiB saved
valid JSON; retryable StepError; and StepRun enqueue commit then original error.
The StepRun SDK Data is 9 bytes shorter than the saved envelope. Mixed8 has two
None descriptors, whose IDs remain pending, and a planned descriptor; its final
pending set is the literal `s0,s5,s7`.

Before measured rows, separate processes run both owners' existing 40-literal
contracts and lifetime/poison/admission checks, then independent port preflights
for every selected lane/profile/workload. Missing Valkey, unknown/duplicate/
partial selectors and wrong counts fail explicitly. No unavailable lane/profile
is substituted. Ordinary package tests skip measurement unless explicitly enabled.

Smoke uses one seed, one warmup per slot, max(2,C) steady requests per row and one
first pair per workload/profile: 60 steady rows, 300 steady requests, 30 separate
first observations and 30 port preflights. It exercises capability only.

## Artifacts, resource bounds and method

Row schema 2 retains every request ID, worker, phase, monotonic slot-start/owner-start/owner-return/delivery/
resource/slot-end offsets in nanoseconds, result class, original result text, callback
count, timeout and completion flag. Samples are preallocated equally and written
outside request timing. A watchdog preserves completed immutable samples and
started/pending IDs before terminating a stuck child. Synchronous FFI calls are
not claimed interruptible. Request deadlines are 20 seconds, row setup/run is
bounded to two minutes, the internal watchdog is 110 seconds after setup, and
the parent process watchdog is 150 seconds. Failure stops the selection and
preserves contaminated rows. No slow/error sample is dropped.

Timestamped `/proc/self/status` and FD snapshots retain whole Go+Rust host RSS,
VmHWM, threads, FDs and goroutines. Baseline/ready/warmup/end/validated/after_close
stages and a 1-ms sampler are retained. The sampler is bounded to 120,000 records;
its perturbation occurs in both lanes. Valkey INFO memory is separate and never
added to host RSS. Load averages, CPU availability, Valkey PING latency and actual
order are recorded. RSS can miss subinterval peaks. Fewer than ten steady RSS
observations, less than ten sampling intervals, or a gap greater than five
intervals makes memory INCONCLUSIVE. No Go allocation metric stands in for native
allocation. Memory eligibility also requires exactly one of each promised
`baseline`, `ready`, `warmup`, `end`, `validated`, and `after_close` snapshot, with
finite positive RSS and positive integer monotonic offsets. Their timestamps
must increase in that lifecycle order: ready strictly precedes every request, warmup
follows first/warmup slot completion and strictly precedes steady slot start, and end
strictly follows steady slot completion. Equality at these three enclosing edges
is insufficient evidence and makes memory INCONCLUSIVE. The warmup boundary exists even for zero warmup
requests. Sampler records are appended after warmup in the raw array; array order
is not timestamp order. Every retained boundary/sample needs usable RSS and a
valid timestamp. Missing/duplicate/invalid records emit explicit row
`rss_inconclusive_reasons`; they cannot supply memory eligibility. Dense steady
samples and bounded-input accounting remain separate necessary conditions.
The peak window remains baseline/ready/warmup/sample/end; validated/after_close
prove lifecycle completion without adding post-row cleanup RSS to that window.
A peak with no usable RSS is null, with a null paired ratio and INCONCLUSIVE
memory, while the other three metrics still derive.

The accepted P2 was eager retention of every first/warmup/steady input through
final validation: controlled run64k/5000 retained 5,033 inputs at C1 and 5,257 at
C8, approximately 329–345 MB of payload. Ready RSS was included in peak, so the
common setup denominator could hide an owner increase under the unchanged 1.20
ratio. Disclosure alone did not repair eligibility. Earlier schema-1 bundles are
historical capability evidence; their memory is now INCONCLUSIVE regardless of
sampling density. Do not modify/rederive the earlier raw bundle in place.

Schema 2 prepares one independent input per occupied worker and releases it
immediately after resource completion and independent validation, before preparing
the next. All mutable opcodes/options/metadata, callbacks, contexts/spans, local
state and run identities remain per request. Inspection of `runGroup`,
`prepareStep`, native construction and callback/drain showed descriptor branch
copies and retained original bindings through completion; no inter-response
payload or descriptor sharing was introduced. Go output preparation and native
projection/copy/codec/allocation/drain remain inside the unchanged owner timing.
Real Valkey runs are still independently precreated and read back before any
requests; precreation constructs metadata without payloads or incoming spans.
Validation/readback happens outside request latency and before input release.

**Throughput scope changed explicitly.** Schema-2 `throughput_scope` is closed-loop
input preparation, owner/resource completion, validation and input release, with
backend runs precreated. The denominator is earliest steady `slot_start_ns` to
latest steady `slot_end_ns`, including all between-call construction, independent
validation/readback, accounting and dispatch gaps in both lanes. Request p95
continues to use `resource_ns-start_ns`. This is harness closed-loop throughput,
not pure owner throughput or end-to-end queue throughput; do not compare it to
schema-1 throughput as an unchanged scope. This setup/validation cost can dilute
owner differences; the separate owner-service-rate guard below is also required,
and both scopes require fresh fairness review before any campaign. No existing
threshold or sampling-density requirement is relaxed. Row `setup_ns`
and `setup_heap_bytes` cover initial backend/record setup, while per-request setup
and validation intervals remain visible through slot timestamps.

Raw `input_retention` counts prepared/validated/released inputs, remaining input
roots, peak roots, peak distinct Data backing-array capacity in bytes and
precreated backend runs. The live-input registry itself owns those roots and is
cleared per completion; no sample, delayed-validation closure, original error
object or recorder retains completed inputs. Data shared among descriptors within
one response is counted once. With these workloads the active payload capacity
is at most C * 65,536 bytes (run64k), otherwise C * 64 bytes; independent input
objects are bounded by C. These are harness ownership counts, not a bound on OS
RSS, transient allocations, garbage awaiting collection or Rust heap activity.
No baseline is subtracted and no precise owner-attribution claim is made.

Scalar outcome/watchdog storage still scales with sample count (maximum 5,257
request identities, with a primary sample array, watchdog copy and up to one
immutable completed sample per identity, plus pointer/start-offset arrays), and
the 120,000-record sampler capacity stays fixed. Both
capacities are recorded explicitly and remain in host RSS; setup heap is only a
Go diagnostic, captured before sampler allocation. Whole-process RSS includes
initial setup, per-request preparation/validation, Go/runtime/native effects and
allocator retention. Memory eligibility requires consistent schema-2 lifecycle
counts, peak input/payload bounds, record capacities and independent backend
precreation counts, as well as unchanged minimum sampling density over steady
slots. Missing, old or over-bound retention evidence makes paired memory and
campaign cells INCONCLUSIVE; latency and disclosed-scope throughput still derive.
Valkey INFO memory remains separately recorded and includes precreated runs plus
retained outputs/queue entries. Use a dedicated disposable Valkey and remove it
after selection. No host isolation, external-load exclusion or cold cache is
claimed.

`scripts/response-owner-measure.py capture` archives tracked and untracked source,
records source/binary/static archive SHA-256 and versions, runs the compiled
binary from the package directory, checks source/binary stability and preserves
commands/logs/row hashes. `derive` reconstructs nearest-rank resource p95 from
all steady durations, and disclosed-scope closed-loop responses/s from earliest steady slot start to
last slot end (schema 2). It verifies IDs/results/boundaries/slot reuse and callback
counts. Ratios pair identical seed/workload/profile/concurrency. Full-campaign
criteria remain UNCALIBRATED: p95 <=1.10, closed-loop achieved throughput >=0.90,
separate owner service rate >=0.90, sampled peak host RSS <=1.20; every one of
30 cells needs median pass and each metric >=2/3 runs.
Sparse memory is INCONCLUSIVE, never PASS. Smoke derivation always says
SMOKE_ONLY_NO_PERFORMANCE_EVIDENCE. First observations stay raw, unpooled.

## Ready commands

Run from this worktree. Use a new cache bundle on each run; retain failures.
The cached accepted Rust archive was copied only after identical Rust source and
Cargo.lock verification against the accepted effects checkout. No Rust source,
ABI, model, fixture answer or production helper changed; no Rust tests/build were
rerun. A clean future checkout needs the already documented native library build
from [real effects](response-real-effects.md), outside owner timing.

```sh
MEASURE_REVIEW_DIR=$(mktemp -d /home/ashutosh/.cache/response-owner-review-20261027-XXXXXX)
mkdir -p "$MEASURE_REVIEW_DIR/tmp"
export GOTOOLCHAIN=local GOMAXPROCS=2
export GOTMPDIR="$MEASURE_REVIEW_DIR/tmp" TMPDIR="$MEASURE_REVIEW_DIR/tmp"
GO=/home/ashutosh/.cache/go1.26.4/go/bin/go
"$GO" test -p=2 -c -tags=response_continuation_spike \
  -o "$MEASURE_REVIEW_DIR/executor.test" ./pkg/execution/executor

# This only writes the full 480-child declaration; it executes no rows.
python3 scripts/response-owner-measure.py capture --mode plan \
  --binary "$MEASURE_REVIEW_DIR/executor.test" --out "$MEASURE_REVIEW_DIR/plan"

MEASURE_VALKEY_NAME=$(basename "$MEASURE_REVIEW_DIR")
sudo -n -u ashutosh -g docker docker run --detach --rm \
  --name "$MEASURE_VALKEY_NAME" --publish 127.0.0.1::6379 \
  valkey/valkey:8.0.3 valkey-server --save '' --appendonly no
MEASURE_VALKEY_PORT=$(sudo -n -u ashutosh -g docker docker inspect \
  --format '{{(index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort}}' \
  "$MEASURE_VALKEY_NAME")
RESPONSE_MEASURE_VALKEY_ADDR="127.0.0.1:$MEASURE_VALKEY_PORT" \
python3 scripts/response-owner-measure.py capture --mode smoke \
  --binary "$MEASURE_REVIEW_DIR/executor.test" --out "$MEASURE_REVIEW_DIR/smoke"
python3 scripts/response-owner-measure.py derive "$MEASURE_REVIEW_DIR/smoke" \
  --output "$MEASURE_REVIEW_DIR/smoke-derived-v3.json"
sudo -n -u ashutosh -g docker docker rm --force "$MEASURE_VALKEY_NAME"
```

The same capture interface accepts `--mode campaign`; this is capability only,
**not approval to execute it**. Independent Luna+Sol fairness review must happen
first. Even accepted FFI measurements cannot select a transport before persistent
process comparison and the unchanged platform/lifecycle/rollout gates.

## Historical pre-P2 verification and handoff

Raw directory: `/home/ashutosh/.cache/response-owner-measure-20261027`.
Final archived smoke: `smoke-final/`, including source.tar.gz, source manifest,
source patch, binary/static hashes, version commands, parent/child command logs,
all raw request/resource records, artifact hashes and `derived.json`.

- Untagged executor package: PASS, 5.298s (`normal.log`).
- Final focused normal: executor 2.762s; FFI adapter 0.006s.
- Final focused race: executor 5.709s; FFI adapter 1.024s.
  Each executes both 40-literal contracts, all retained lifetime/poison/admission/
  callback tests, the existing six actual Valkey cases in both key-queue modes,
  relevant staged/step coverage and the new measurement regressions. No skips.
  Exact argv/environment, UTC times, exits and log hashes are in
  `validation-commands.json`; output is `final-focused.log`/`final-race.log`.
- The three Go overlay negative controls bypass callback drain, report owner time
  as resource completion, and accept invalid selectors. Each exits 1 at the
  intended contract assertion, not compilation: `cleanup began before callback
  delivery`, `early completion timestamps`, `accepted selector`. Overlay sources
  and JSON live in `mutations/`; raw logs/commands are retained. No repository
  source was mutated for these controls.
- Actual CLI unknown `ffi` selection exits 1; requested Valkey without an address
  exits 1 in port preflight with `requires RESPONSE_MEASURE_VALKEY_ADDR; no
  fallback`. See `negative-selector.log`, `negative-profile.log` and
  `missing-profile/`. The dry plan exits 0 with 180 steady rows/360,000 steady
  responses and 300 separate first-observation children; `plan/selection.json`.
- Final captured smoke exits 0 in 28.37s: 30 port preflights, 60 steady rows/300
  steady responses, one warmup per slot, one first per row and 30 separate first
  observations. This is 690 harness requests including warmup/preflight/first,
  plus the separately retained literal/lifetime preflight. Both lanes/profile/
  workload/concurrency selectors and both actual Valkey queue modes are covered.
  The source and both binary fingerprints remained stable during capture.
  Derivation reconstructs raw durations and 30 paired cells, but always labels
  the bundle SMOKE_ONLY_NO_PERFORMANCE_EVIDENCE. RSS is INCONCLUSIVE in 57/60
  smoke rows. No threshold verdict or speed/memory claim is made.
- Artifact hashes were independently rechecked. The factored host and retained
  integration witnesses compare byte-for-byte to base excluding imports/trailing
  whitespace. `git diff --check` passes. The plan-status script exits 1 for the
  pre-existing invalid `run-ai-debugging.md` status; that file is unchanged.
- Rust source/Cargo.lock match the accepted cached artifact source; no Rust build
  or tests were run. The static archive SHA-256 remains
  `562139fab7fec3ed44cdcb596276c1241d038ed95c0d34a165fa7d6d0f28ed7d`.
  Final executor.test SHA-256 is
  `9b5f0b520eb00c1458e014825a29a99fe153324c49222ee1034eecb9388280d6`;
  source.tar.gz SHA-256 is
  `8ec4d7a89de1c2e8dfab06d6a57f1ef96fe8c304c927e88d1b53881f6640c58e`.
  Final evidence-only plan/status/reference updates follow the archived snapshot;
  implementation and binary are unchanged afterward.

Earlier failed development smoke directories (`smoke-01` through `smoke-05`,
plus `smoke-bundle`) are preserved, not filtered out or treated as passing rows.
They exposed wrong launch cwd, pending-set expectation, own cleanup cancellation
misclassified as timeout, incorrect metadata delta/precreation expectations, and
missing input queue-item concurrency keys. The last was detected by strengthening
persisted binding readback. These were harness/setup repairs, not production
changes or attempts to rerun favorable campaign cells. `smoke-06` was an earlier
passing capability smoke before final artifact/readback improvements. These
iterations are not performance evidence and do not have final-source fingerprints.

The disposable `response-owner-measure-20261027` Valkey 8.0.3 container was removed;
its exact image fingerprint/port record is `valkey-image.txt`. No other container
was removed. Native arm64/final Cgo link, persistent process comparison,
checkpoint/CritT/Stop/fencing/lost-ack and broader migration gates remain open.
Independent Luna+Sol fairness review is the next handoff, not performed here.
No full E2E, benchmark campaign, production activation, commit, push, PR or
review delegation is authorized in this implementation turn. Leave uncommitted.

## P2 repair verification and handoff (2026-10-27)

New evidence root:
`/home/ashutosh/.cache/response-owner-memory-p2-20261027`.
Earlier `/home/ashutosh/.cache/response-owner-measure-20261027` artifacts remain
immutable. The resource-regression authoring gate protects actual row input-root
lifetimes as sample counts grow; existing literal/completion tests cannot observe
this retention. The reducer regression protects dense yet setup-bloated RSS
ratios from reaching PASS. It uses synthetic numbers solely to test verdicts,
not to predict or guarantee OS memory. Neither adds a production seam.

Fresh Luna review remains pending; no improvement claim, full campaign, production
change, Rust model/ABI change, fixture update, delegation, commit, push or PR.

- Normal untagged executor package PASS (5.277s). Focused normal executor/FFI
  PASS (7.406s/0.006s); focused race PASS (41.185s/1.027s), including both
  40-literal contracts, integrated lifetime/poison/admission, both actual Valkey
  queue modes, staged/step, selectors and completion checks. Exact commands,
  times, exits and log hashes: `validation-commands.json`, `normal.log`,
  `focused.log`, `focused-race.log`. No full E2E/campaign or Rust tests/build.
- `TestResponseMeasureInputRetention` executes real controlled run64k rows for
  both lanes, C1/C8, N=8/80 and two warmups per slot. All lifecycle totals match
  11/83 requests (C1) and 25/97 (C8), with zero remaining inputs. Each lane
  observes peak 1 input/65,536 bytes at C1 and 8/524,288 at C8 for both counts.
  These are owned-input accounting observations, not promised RSS values.
- `retention-negative.py` overlays the original eager row lifetime, retaining
  actual input roots during setup and recording actual validation/release
  events. The new regression exits 1 at `sample-count-dependent input retention`
  with peak 11 inputs/720,896 bytes for C1/N8. It does not fail on compile or
  timing assertions. Primary proof: `negative-eager-lifecycle.log`,
  `negative-eager-lifecycle-command.json`, `mutations/eager-lifecycle.go`/`.json`.
- `python3 scripts/test-response-owner-measure.py -v` PASS. Dense synthetic RSS
  350/380 MB (ratio <1.20) remains INCONCLUSIVE for legacy, eager, payload-only,
  unreleased or missing-backend-precreation evidence; bounded evidence is an
  eligible control. Latency and disclosed-scope throughput still derive and
  the denominator is checked independently. Bypassing only retention eligibility
  exits 1 at PASS-versus-INCONCLUSIVE assertions: `negative-memory-eligibility.log`
  and matching command JSON/`mutations/` sources. This is reducer logic evidence,
  not an OS memory measurement or owner allocation estimate.
- Rebuilt `executor.test`; binary SHA-256
  `4124474f485d544e3a4cd9e11350009770387832628b80cdcd9dcfd78c0ae678`.
  Native archive remains unchanged at
  `562139fab7fec3ed44cdcb596276c1241d038ed95c0d34a165fa7d6d0f28ed7d`.
  Unknown CLI lane and requested missing Valkey fail at their intended guards.
  `plan/` archives the declaration only: 180 steady rows/360,000 responses and
  300 separate first children; nothing from that campaign ran.
- `smoke-bounded/` passes the tiny full selector matrix (28.55s): 30 independent
  port preflights, 60 steady rows/300 steady responses and 30 separate first
  observations. All steady rows have BOUNDED_INPUTS evidence; 54/60 have sparse
  RSS sampling. All 369 bundle hashes pass and source/binary/native fingerprints
  remain stable through capture. Status is SMOKE_ONLY_NO_PERFORMANCE_EVIDENCE.
  This first new bundle also archived generated Python bytecode; the bytecode
  was then removed from the checkout, and the reducer test now suppresses its
  creation. The final cleaned-source smoke snapshot is retained separately as `smoke-final/`.

Reproducible retention checks are in the focused test command above; reducer
checks use the repository script. Full capture commands are in each bundle's
manifest. The initial negative-control prototype used manually assigned lifecycle
totals (`negative-eager-inputs.log`/`mutations/eager.go`); it is superseded by the
actual-lifecycle negative control above and is not proof for this fix. No output
from either smoke is a performance report. Final evidence-only documentation
updates may follow the archived source snapshot; Go implementation/binary must
remain identical to that snapshot.

Final cleaned-source `smoke-final/` passed in 28.35s with the same complete tiny
selector matrix: 60 steady rows, 300 steady requests, 30 separate first
observations and 30 independent port preflights. All 60 steady rows have bounded
input accounting; 55/60 are RSS-INCONCLUSIVE. `verify-final.py` independently
reconstructs all 60 resource p95 and expanded-scope throughput values, verifies
the 30 paired cells and all lifecycle/precreation bounds, and rechecks all 369
hashes in both the new and untouched earlier final bundles. Its output is
`final-verification.json`. Status stays SMOKE_ONLY_NO_PERFORMANCE_EVIDENCE.
Final source archive SHA-256:
`ab7e45b68ef64a50932d53e3eec71772ba86a826bb78af89a53b2c8d37ef6e88`.
Binary and native hashes are the ones above; all capture fingerprints stayed
stable. This final paragraph and status evidence were added after capture; the
Go implementation, reducer and binary match the archived snapshot. Earlier new
`smoke-bounded/` also remains preserved. The owned disposable Valkey container
was removed; its image ID/port are in `valkey.json`. `scope-verification.json`
compares authored sources against the earlier archived baseline: only the two
measurement Go files, reducer plus new reducer regression, and task documentation
changed. The factored driver, integration witnesses, all production Go, Rust,
Cargo.lock and fixture hashes remain unchanged (generated prior Python bytecode
is excluded from authored-source comparison). `git diff --check` passes. The
plan-status script still exits 1 for the pre-existing invalid
`run-ai-debugging.md` status; that file is unchanged. Campaign stays on hold;
fresh Luna review of retention and the explicitly expanded throughput scope is
the next step. No improvement or owner-memory attribution is claimed.

## P2 throughput-scope repair (2026-10-27)

Accepted finding: closed-loop harness work can mask owner service occupancy even
when resource p95 and eligible memory pass. The independent synthetic regression
uses exactly 100 steady C1 requests per lane: Go 100 x 1 ms, native 96 x 1 ms plus
4 x 7 ms, with equal 2 ms harness overhead per request. Three matched seed blocks
have p95 ratio 1.0, closed-loop achieved-throughput ratio 300/324 = 0.925926,
and memory ratio 1.0 with dense RSS and bounded-input eligibility. The pre-fix
reducer returns PASS; the fixed reducer returns FAIL because the separate owner
service-rate ratio is 100/124 = 0.806452. This tests the verdict contract, not OS
memory or real owner performance. The existing bounded-memory control already
checks a normal PASS; no second mirrored positive fixture was added.

For each steady row, `owner_service_rate_per_second` is
`N * 1e9 / sum(resource_ns-start_ns)`: inverse mean resource service duration,
compared native/Go within the same seed/workload/profile/C. At equal N this ratio
is Go summed resource occupancy / native summed resource occupancy. It is a
service-rate guard, not achieved throughput, a CPU-time metric, an estimate of
maximum C8 capacity or an owner-attributed wall rate. It retains callback,
resource drain and scheduler delay inside the existing resource boundary. No
harness cost is subtracted from a global wall measurement. Both throughput
criteria must independently meet >=0.90; p95 <=1.10 and peak RSS <=1.20 remain.
All four metrics require a passing median paired ratio and >=2 of the three
seed blocks in every cell. Zero, negative or missing resource durations stop
derivation before any output, including first/warmup samples. Missing memory
eligibility still prevents PASS. Thresholds remain uncalibrated.

Existing schema-2 timestamps suffice; capture, Go measurement files, driver,
input lifetimes, production Go/Rust and fixtures were not changed. Derived
schema 3 reports steady `wall_elapsed_ns`, `owner_resource_occupancy_ns` and
`steady_cost_sums` separately:

- preparation: sum of `start_ns-slot_start_ns`;
- validation/release: sum of `slot_end_ns-resource_ns`, including input release
  and bookkeeping, without claiming validation and release can be split;
- owner/resource: sum of `resource_ns-start_ns`;
- slot occupancy: sum of per-request slot durations, equal to the first three;
- dispatch gap: sum of next slot start minus previous slot end on the same
  worker, between consecutive steady requests only, with a separate gap count.

These are elapsed intervals summed across workers, not wall elapsed or pure CPU
costs. C8 overlap makes a sum exceed wall time legitimately. Dispatch excludes
warmup-to-steady transitions and worker edge idle time; no global subtraction
estimates those missing intervals. In this tiny smoke C8 has one steady call
per worker and therefore **zero observed steady dispatch gaps**, not evidence
that dispatch is free. C1 has one observed gap per row. Schema-1 rows lack these
slot cost boundaries and report null costs, never invented values.

Evidence root: `/home/ashutosh/.cache/response-owner-throughput-p2-20261027`.
The source is the untouched
`/home/ashutosh/.cache/response-owner-memory-p2-20261027/smoke-final` bundle.
`smoke-derived-v3-final.json` records the source path, manifest/selection/every raw-row
SHA-256, reducer SHA-256, original base and binary SHA-256. CLI `derive` now
requires an explicit new `--output` path; existing destinations are refused.
Capture writes its initial derived file exclusively in its newly created bundle.
Raw schema and historical derived results remain unchanged.

- `regression-before.log` / `regression-before-command.json`: exit 1 on the
  actual PASS-versus-FAIL assertion before the fix; the zero duration was also
  incorrectly accepted. Negative durations already stopped at boundary ordering,
  and a missing timestamp already stopped with KeyError; the fixed reducer
  explicitly rejects all three with a resource-duration diagnostic.
  `before-response-owner-measure.py` and `counterexample-before-test.py` preserve
  the reducer and test used for that negative control.
- `regression-after.log` / matching command JSON: all three reducer tests PASS,
  exit 0, including the exact counterexample, duration rejection and retained
  memory-eligibility/normal-pass control. `counterexample.json` records all three
  paired blocks plus the actual pre-fix and post-fix cell verdicts.
- `derive-smoke-final-command.json` / `.log`: exit 0, 60 steady rows / 300 requests,
  30 matched pairs. No campaign cells or performance verdict are manufactured;
  status remains `SMOKE_ONLY_NO_PERFORMANCE_EVIDENCE`, with 55/60 sparse RSS rows.
- `verify.py` independently reconstructs the p95, achieved throughput, summed
  costs, inverse-mean service rates and paired ratios. `verification.json`
  confirms every earlier derived row/pair value remains identical, all 60 input
  accounts are bounded, all 370 source-bundle files are unchanged, and all 369
  recorded artifact hashes verify. Its per-profile/C/lane cost summaries are
  diagnostics of this smoke only. Across the ten Go rows per Valkey profile,
  median preparation plus validation/release is 44.33% (off) / 46.32% (on) of
  summed slot occupancy. That substantial harness work motivates the separate
  service-rate criterion; it is not a measured improvement.
- `scope-verification.json` confirms archived Go/Rust/driver/fixture sources,
  cached Go binary SHA-256
  `4124474f485d544e3a4cd9e11350009770387832628b80cdcd9dcfd78c0ae678`
  and native archive SHA-256
  `562139fab7fec3ed44cdcb596276c1241d038ed95c0d34a165fa7d6d0f28ed7d`
  remain unchanged. No binary changed, so no rebuild, fresh smoke or Go harness
  checks were run for this reducer-only repair.

Reproduction from the worktree (choose an unused output path):

```sh
python3 scripts/test-response-owner-measure.py -v
python3 scripts/response-owner-measure.py derive \
  /home/ashutosh/.cache/response-owner-memory-p2-20261027/smoke-final \
  --output /home/ashutosh/.cache/response-owner-throughput-p2-20261027/smoke-reproduced-v3.json
python3 /home/ashutosh/.cache/response-owner-throughput-p2-20261027/verify.py
```

The original samples and binaries are local cache artifacts, not committed
portable performance evidence. A new derived file records the current reducer
hash; byte-for-byte reproduction requires the same reducer and source path.
Tiny counts, one seed, loaded host, sparse RSS and unobserved C8 steady gaps
prevent performance conclusions or campaign acceptance. Fresh guard/cost-scope
review and concurrent read-only Luna memory-depth review remain pending. Leave
all work uncommitted. Entire campaign remains on hold; no improvement claim,
production change, delegation, commit, push, PR, activation or bridge selection.


## P2 boundary-snapshot repair (2026-10-27)

Accepted Luna P2: the reducer selected any present baseline/ready/warmup/sample/end
records without requiring boundary evidence, so dense samples alone could yield
memory PASS. The former reducer fixtures contained only sample records, including
the throughput counterexample; these did not represent the promised full raw
contract. Capture inspection (`stage` in `response_continuation_measure_test.go`)
confirms all six stages described above are unconditional for completed rows.
This repair changes only the reducer, its regression tests and task docs. Capture,
Go/Rust, driver, binaries and literal fixtures remain untouched.

The reducer now requires all six boundaries and validates their monotonic
lifecycle ordering and request enclosure. Every boundary/sample must have finite
positive RSS; zero, negative, absent, NaN and positive/negative infinity cannot
pass. Invalid evidence produces explicit reasons, independently of the unchanged
sampling-density and bounded-input guards. Missing memory evidence does not stop
latency, closed-loop throughput or inverse-mean owner-service-rate derivation.
No thresholds changed: p95 <=1.10, closed-loop >=0.90, owner service rate >=0.90,
memory <=1.20, median plus >=2/3 passing seed blocks for every metric/cell.

The authoring gate retains the raw-row reducer as the primary test owner: these
regressions protect the protocol's evidence eligibility and verdict, which the
Go lifetime and timestamp tests do not cover. No production seam is introduced.
Shared synthetic boundary setup now gives both existing regression owners full
valid lifecycle evidence; synthetic RSS tests verdict logic only. The 100-request
counterexample remains exactly 100 steady C1 requests, with a tiny untimed gap
between first and steady to permit the real warmup boundary. Both owners' steady
ratios remain exactly the prior values.

Evidence root: `/home/ashutosh/.cache/response-owner-boundary-p2-20261027`.

- `negative-test.py` runs the corrected fixtures against preserved
  `before-response-owner-measure.py`; `negative-prefixed-reducer.log` exits 1
  with 81 failed assertions and seven errors across the 88 evidence controls.
  Missing stages, samples-only and invalid RSS reproduce SAMPLED-versus-INCONCLUSIVE;
  absent RSS also demonstrates the old KeyError rather than a usable diagnostic.
  Existing retention and throughput tests still pass on that pre-boundary reducer.
- `python3 scripts/test-response-owner-measure.py -v` exits 0; all four test
  methods pass (`regression-after.log`). The evidence table covers each missing,
  duplicate, misordered and invalid-time boundary; absent/zero/negative/NaN/+Inf/-Inf
  RSS for each boundary and sample; samples-only, sparse, empty and wholly unusable
  RSS. Each control checks explicit reasons, memory INCONCLUSIVE and continued
  availability/passing verdicts of the other metrics. The bounded normal PASS,
  legacy/eager/payload/unreleased/backend retention controls, and zero/negative/
  missing resource-duration errors remain covered.
- `counterexample.json` independently records all three seed pairs and actual
  cell verdict: eligible memory PASS, p95 1.0, closed-loop 0.925926, owner service
  rate 0.806452 (0/3 passing), overall FAIL. Missing memory does not mask this test.
- Recompute command, exit 0:
  `python3 scripts/response-owner-measure.py derive /home/ashutosh/.cache/response-owner-memory-p2-20261027/smoke-final --output /home/ashutosh/.cache/response-owner-boundary-p2-20261027/smoke-derived-v3-boundaries.json`.
  The new schema-3 result retains raw manifest/selection/row and reducer hashes,
  original base and binary provenance. All 60 steady rows/300 requests and 30
  pairs rederive. Five rows remain SAMPLED and 55 INCONCLUSIVE. **No memory
  classifications changed:** every raw steady row already has all six usable,
  correctly timed boundary snapshots; those 55 rows fail only the unchanged
  density requirement. Every earlier row value and pair matches the prior
  schema-3 result; the new row reasons explain each inconclusive classification.
- `python3 /home/ashutosh/.cache/response-owner-boundary-p2-20261027/verify.py`
  exits 0 (`verification.json`): both historical final bundles retain all 370
  files each and all 369 sealed hashes each (738 total). It also compares the
  earlier throughput repair's recorded raw hashes; all 1,969 snapshotted raw,
  authored Go/Rust/fixture and binary/native paths remain unchanged. The cached
  Go binary and native archive retain the hashes recorded in the previous repair.
  No manifest-sealed output was overwritten. `git diff --check` passes.

Cumulative findings: input-retention denominator P2, owner-service dilution P2,
and missing-boundary memory P2 are implemented and locally verified. This is
**not full readiness acceptance**. The parent must commission fresh combined
Luna memory and Sol throughput final review, including these cumulative repairs.
The campaign remains on hold. No capture/rebuild/Go rerun, delegation, commit,
push, PR, campaign, production activation or bridge selection was performed;
all work remains uncommitted. Broader platform/lifecycle/transport gates remain.


## P2 strict boundary enclosure follow-up (2026-10-27)

Accepted Luna memory round-three finding: equality at ready/first-slot start,
warmup/first-steady-slot start, or end/last-steady-slot completion was accepted
despite the promised before/after enclosure. Only these three comparisons are
now strict; each diagnostic names the required strict ordering. All other
conditions, formulas, service-rate guard and thresholds remain unchanged.

The existing raw-row boundary regression is the primary owner of three new
equality controls (91 controls total, retaining the earlier 88). They reproduce
the missing strict enclosure independently while all other evidence is valid;
existing misordered-stage cases did not exercise equality with request edges.
Each yields exactly one explicit reason and memory INCONCLUSIVE, with unchanged
latency/throughput/service values and passing nonmemory cell verdicts. No test
seam, Go/Rust/capture source or binary change is required.

Evidence: `/home/ashutosh/.cache/response-owner-strict-boundary-p2-20261027`.

- `python3 <evidence>/negative-test.py` uses the preserved prior reducer with
  current tests: exit 1, exactly three failures at SAMPLED versus INCONCLUSIVE,
  one for each equality. All earlier controls and other test methods pass;
  `negative-command.json` and `negative-prior-reducer.log` preserve the result.
- `python3 scripts/test-response-owner-measure.py -v`: exit 0, all four methods
  pass (`regression-after.log`). The eligible-memory service counterexample
  remains overall FAIL: p95 1.0, closed-loop 300/324, service 100/124 = 0.806452,
  zero passing service runs, memory PASS (`verification.json`).
- Fresh derivation (exit 0), without capture or overwriting sealed artifacts:
  `python3 scripts/response-owner-measure.py derive /home/ashutosh/.cache/response-owner-memory-p2-20261027/smoke-final --output /home/ashutosh/.cache/response-owner-strict-boundary-p2-20261027/smoke-derived-v3-strict-boundaries.json`.
- `python3 <evidence>/verify.py`: exit 0. All 60 raw steady rows already strictly
  enclose the three edges. All 60 rows/300 requests/30 pairs and every derived
  value match `response-owner-boundary-p2-20261027/smoke-derived-v3-boundaries.json`;
  only provenance's reducer hash differs. Five SAMPLED/55 INCONCLUSIVE remain.
  All 10,020 snapshotted Go/Rust/Cargo/raw-bundle/prior-derived paths and all
  369 sealed raw-bundle hashes remain unchanged. Equality controls separately
  compare every row/pair numeric value before/after (`verification.json`).
- Reducer SHA-256: `118ce309cf80dd5f4fab202d719a003466db3a01af1d4c926cad3db64ca71310`.
  Test SHA-256: `108897adae6ee0aec3103f3d3b2d105edc4b56fde8ebe6c494661372310297ef`.
  New derived SHA-256: `50cdce640cc6424a48b84dcf21ed0d58537ed83fd36f53080e3ce4e40793af0c`.

Local repair evidence only; readiness remains pending parent acceptance and fresh
combined review. Concurrent Sol throughput review stays read-only. Campaign stays
on hold; no Go/Rust build, capture, delegation, commit, push or PR. Leave uncommitted.


## Later parent authorization and stopped campaign attempt (2026-10-27)

The parent later accepted the cumulative harness reviews and authorized one declared campaign attempt. It stopped at the first steady row under the existing 110-second watchdog, with an incomplete native run64k/C1 sample. All functional and port preflights passed. No steady cell or performance gate was derived, no row was rerun, and no bridge was selected. The complete failure evidence, exact commands, hashes, environment, and next independent review gate are in the [stopped campaign report](response-owner-measurement-campaign-2026-10-27.md) and its [machine-readable audit](response-owner-measurement-campaign-2026-10-27.json). This supersedes the earlier authorization hold only for that one attempt; further campaign work remains subject to independent Sol review and separate authorization.


## Accepted capture-budget diagnostic amendment — 2026-10-27

The parent accepted the Sol stop audit: the first campaign row made sustained
progress through 110 seconds (3,419 complete of 5,033; last completions
109.919/109.954/109.990s; next request in flight <48ms). Read the old raw
`stuck`/`timeout=true` as “in flight when row watchdog expired; request
timeout/native hang not established.” Preserve that bundle and its raw fields.
The 161.9–167.4s extrapolation is for diagnostic capture budgeting only.

`capture --mode diagnostic` is a new, fixed bounded profile: exactly seed 26011,
native/controlled/run64k/C1, first + 32 warmups + 5,000 steady requests = 5,033.
It validates exact selectors (the Python wrapper supplies them if omitted), uses
GOMAXPROCS=8, and records the profile in both manifest and raw row. Its budgets
are post-setup watchdog 210s, row context 240s, Go test timeout 260s, external
process-group kill 270s, per-request deadline 20s, sampler cap 240,000 records.
There are no arbitrary timeout/count overrides or resume mode. Campaign defaults
remain 110/120/140/150s and 120,000 records; the 20s request deadline is unchanged.

The diagnostic executes one row directly in the test process. Run the existing
literal/lifetime/poison/admission functional suite and focused capture-control
checks separately before capture, with exact commands/statuses in the new bundle.
It does not run the campaign's 30 port-preflight rows or separate first-observation
children. Controlled effects need no Valkey; no container is required.

New failures distinguish row-watchdog expiry, observed request-deadline expiry,
row-context expiry and sampling errors. No new watchdog path invents a request
timeout or establishes a native hang. Stop dispatch on first observed error;
retain published scalar completions, current input accounting, and available
lifecycle/RSS records on failure. A 50ms deadline observer bounds unreturned
requests after their 20s deadline without promising native cancellation.
Failure-only diagnostics write an all-goroutine Go stack bounded to 2MiB and
record possible truncation; if already installed, gdb gets a bounded 3s/2MiB
backtrace attempt (plus 1s pipe-drain limit), without installation/elevation.
Tool/ptrace/symbol/unwind availability limits native evidence. External kill or
a failure of the Go runtime may prevent these best-effort writes.

The diagnostic completion summary validates raw slots/callbacks/retention and
lifecycle resources without computing latency percentiles, service/throughput
rates, ratios, comparative gates or a campaign PASS. Existing RSS density and
strict boundary rules stay unchanged; sparse sampling is explicitly INCONCLUSIVE.
The comparative reducer refuses diagnostic bundles. One success or failure
exhausts this authorization; no automatic retry or matrix continuation.

See the [one-row diagnostic reference](response-owner-diagnostic-2026-10-27.md)
for exact source changes, authoring gate, commands, hashes, results and limits.
Phase 1 remains partial; Phase 2 remains open; bridge choice stays unset.


### One diagnostic result (authorization exhausted)

The single fixed replay exited 0: all 5,033 unique slots validated and closed,
5,033 callbacks completed, native close/drain succeeded, zero request timeouts
and zero retained inputs. Last slot closed at 156.080s for budget accounting
only. All six lifecycle snapshots and 152,453 periodic resource records were
retained. RSS remains INCONCLUSIVE because its 17.352ms maximum steady sampling
gap exceeds the unchanged 5ms limit. No comparative metrics, paired gates or
campaign verdict were computed, and no second row ran. Focused validation,
exact commands, preserved initial command-cwd failure, negative control, hashes
and limits are in the [diagnostic reference](response-owner-diagnostic-2026-10-27.md).
The parent accepted the harness and diagnostic evidence with limited scope. Matched-port, timing-boundary and raw-reproduction reviews were accepted; bounded input retention was accepted; Sol's service-guard review 03 found no blockers; the parent inspected all six RSS-boundary guards and the strict-enclosure edge fix; Luna's final diagnostic control/source/evidence review found no blockers.

The campaign itself is still incomplete. The first native/controlled/run64k/C1
row expired at 110 seconds after 3,419 of 5,033 requests completed. Preserve its
original `stuck`/`timeout=true` raw fields as the old watchdog classification;
sustained progress was observed and no request timeout/native hang was
established. The sole diagnostic replay closed all 5,033 slots at 156.080s with
zero retained inputs under 210/240/260/270s watchdog/context/test/external
budgets and the unchanged 20s request deadline. Campaign defaults remain
110/120/140/150s and 120,000 periodic records. The 152,453 periodic samples have
a 17.352ms maximum gap, above the unchanged 5ms limit, so RSS is INCONCLUSIVE.
Whole-host RSS is not precise Go-versus-Rust attribution. No comparative gates,
transport selection or activation were derived. The original failed bundle's
118 files/hashes and the initial wrong-cwd functional command failure remain
preserved. The one-row authorization is exhausted; the next measurement
decision is pending separate authorization. Broad Phase 1 remains partial;
Phase 2, persistent-process comparison, native arm64 and activation gates stay
open. The detailed [STOP report](response-owner-measurement-campaign-2026-10-27.md)
and the [structured diagnostic audit](response-owner-diagnostic-2026-10-27.json)
retain raw paths, hashes, commands and derivations.
