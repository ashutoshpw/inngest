# One-row response-owner capture diagnostic — 2026-10-27

**State:** Exactly one replay complete; Luna's final control/source/evidence review found no blockers and the parent accepted this diagnostic evidence only. No performance verdict or full campaign authorization remains; the next measurement decision is pending.
**Checkout:** `/home/ashutosh/.t3/worktrees/inngest/t3-0e6fce65`
**Branch/base:** `t3/response-owner-measurement-harness` / `f9cf1892c0de4e1c9a6cedd8cbcfe8e2473e5ae0`.
**Evidence:** `/home/ashutosh/.cache/response-owner-diagnostic-20261027-dvs1uhhh`.

## Authorization and preserved failure

The parent explicitly accepted Sol’s stop audit: 3,419 complete of 5,033 with
sustained progress until 110s; last completions 109.919/109.954/109.990s; next
request in flight <48ms. In flight when row watchdog expired; request
timeout/native hang not established. The old `stuck`/`timeout=true` raw fields
are immutable, including the entire original bundle at
`/home/ashutosh/.cache/response-owner-review-20261027-jSVKc2`.
The estimated 161.9–167.4s is diagnostic budgeting only.

Authorized exactly once: seed 26011 / native / controlled / run64k / C1,
GOMAXPROCS=8; 32 warmups + first + 5,000 steady = 5,033 slots. Post-setup
watchdog 210s; row context 240s; test timeout 260s; external kill 270s; unchanged
20s per-request deadline; at most 240,000 periodic resource records.
No full campaign, retries, matrix continuation, optimization, production/model/
codec/fixture changes, comparative derivation, bridge decision or delegation was
authorized. Repository delivery of the accepted harness/diagnostic tooling and
documentation is covered by the current implementation request; that delivery
does not authorize further measurement.

## Source scope and authoring gate

- `response_continuation_measure_test.go`: named fixed diagnostic budget and
  exact-row selection; direct one-process diagnostic; current failure snapshots;
  truthful stop reasons; bounded failure stacks; stop dispatch on first error.
- `scripts/response-owner-measure.py`: fixed diagnostic CLI/limits and provenance,
  external kill, host environment/quota evidence, completion-only validation.
  Comparative formulas/thresholds remain unchanged and refuse diagnostic mode.
- Focused Go capture-control checks protect exact authorization, coordinated
  budgets, preserved campaign defaults, truthful failure classification and
  bounded output. Existing selector/completion-boundary tests do not cover
  these capture-control contracts. A regression could accidentally expand scope,
  time out before the accepted budget, fabricate request timeout, or allow
  unbounded debugger output. No production hook or new model behavior is needed.
- Python diagnostic checks exercise the actual completion validator with a
  synthetic 5,033-slot fixture and corrupt timeouts/callbacks/slot reuse/phases/
  budgets/retention/resources. They also reject scope expansion and comparative
  derivation. Existing paired-reducer tests cannot cover the new mode.
- Measurement boundaries, callback validation, payload ownership, native/session
  close paths, input preparation and codecs remain owned by the existing code.

## Validation and execution

A fresh binary uses cached Go 1.26.4, cache-backed temp space and build parallelism
`-p=2` / GOMAXPROCS=2. Reuse the existing Rust 1.94 archive only after source/
Cargo.lock and hash comparison to the stopped capture; no native rebuild.
Focused functional checks run separately before capture. The diagnostic captures
exactly one row, so no measurement port preflights or separate first children;
controlled effects need no Valkey runtime and no containers are started.

Commands, hashes, results and limits from the one attempt follow below.
All original-bundle hashes and source-scope verification are retained in this
new evidence root. The captured source snapshot will precede final result-only
task-document updates. Phase 1 partial; Phase 2 open; bridge unset.


## Validation results and exact commands

All commands ran in the designated checkout. Build and Go-tool checks used cached
Go 1.26.4, `GOTOOLCHAIN=local`, `GOTMPDIR` and `TMPDIR` set to the evidence
root’s `tmp/`, and `-p=2`. Build GOMAXPROCS=2; functional/capture-control/race/
capture GOMAXPROCS=8. `PYTHONDONTWRITEBYTECODE=1`; Valkey address and child
markers were unset. Exact argv/cwd/start/end/status/environment are in
`*-command.json` at the evidence root; each has its corresponding `.log`.

```sh
# Fresh final capture binary: exit 0.
/home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 -c \
  -tags=response_continuation_spike \
  -o /home/ashutosh/.cache/response-owner-diagnostic-20261027-dvs1uhhh/executor-diagnostic.test \
  ./pkg/execution/executor

# Six Python test methods: exit 0 (reducer-checks-final).
python3 scripts/test-response-owner-measure.py

# Three normal control/boundary/selector tests: exit 0 (capture-controls-final).
/home/ashutosh/.cache/response-owner-diagnostic-20261027-dvs1uhhh/executor-diagnostic.test \
  '-test.run=^TestResponseMeasure(CaptureControl|Selectors|CompletionBoundaries)$' \
  -test.v -test.timeout=90s

# Same three tests under race: exit 0 (capture-control-race).
/home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 -race \
  -tags=response_continuation_spike ./pkg/execution/executor \
  '-run=^TestResponseMeasure(CaptureControl|Selectors|CompletionBoundaries)$' \
  -count=1 -v -timeout=90s

# Existing functional preflight suite: 14 top-level tests pass (exit 0).
# This binary command requires cwd pkg/execution/executor.
/home/ashutosh/.cache/response-owner-diagnostic-20261027-dvs1uhhh/executor-diagnostic.test \
  '-test.run=^TestResponseContinuation(Contract|Integrated(Contract|RejectsWholeBatch|SiblingProgressAndJoin|Close.*|RecoveredPanic.*|Poison.*|RetainsFullDescriptor|ReleasePort|Activation|AsyncPanicSemantics))$' \
  -test.v -test.timeout=90s

# The ONE authorized replay, exit 0; shown as executed, not a new authorization.
python3 scripts/response-owner-measure.py capture --mode diagnostic \
  --binary /home/ashutosh/.cache/response-owner-diagnostic-20261027-dvs1uhhh/executor-diagnostic.test \
  --out /home/ashutosh/.cache/response-owner-diagnostic-20261027-dvs1uhhh/capture \
  --go /home/ashutosh/.cache/go1.26.4/go/bin/go \
  --lanes native --profiles controlled --workloads run64k --concurrency 1
```

The capture manifest also records Go 1.26.4, Rust 1.94.0 and C compiler version
commands (all exit 0), exact binary argv, package cwd, and the fixed 260s Go
timeout. The Python parent applies the 270s external process-group kill.

`budget-mutation-command.json` records an expected exit 1: a cache-only Go
overlay changed the diagnostic watchdog back to 110s. The bounded-authorization
check failed with “uncoordinated diagnostic budget”, proving that the old
budget is rejected for this new mode. Checkout source was not mutated.
An initial standalone functional command from the repository root exited 1
only because two fixture-relative paths could not be opened. Preserve
`functional-command.json`/`functional.log`. The corrected package-cwd command
passed all 14 tests (`functional-cwd-corrected-*`). No diagnostic row had run
at that point. An earlier fresh development binary is also preserved; the
final build, final normal/race checks and capture all use the final source.

## Single-row result

`capture/rows/selection.json` contains exactly one declaration and
`capture/rows/row-000.json` contains exactly 5,033 unique requests: first 1,
warmup 32, steady 5,000. All 5,033 have successful result, callback count 1,
complete owner/delivery/resource boundaries and a closed slot; zero request
timeouts and no row error. All 5,033 were independently validated and released,
with zero retained inputs, peak one input / 65,536 payload-capacity bytes, and
zero precreated backend runs. Slot reuse followed resource cleanup and validation.

For this native lane, `complete=true` requires callback drain and successful
`d.drain()` (including native `Session.Close`, watcher completion and callback
wait) before resource completion. All callbacks completed and every session
close returned successfully; no session/callback lifetime leak was observed.
There is no new allocator instrumentation or broad native-heap leak claim.

All six lifecycle snapshots are retained: baseline, ready, warmup, end, validated,
after_close. All have usable RSS and ordered monotonic times. There are 152,453
periodic RSS/FD/thread/goroutine records (152,459 total including boundaries),
below the 240,000 periodic-record bound. FD stayed 6 at all boundaries; threads
stayed 12. Goroutines were 6 at baseline/ready, 8 at warmup and 7 at end/validated/
after_close; the capture watchdog is still live at these last snapshots, then
is stopped and joined before the row returns. The measurement process exited 0.

**RSS remains INCONCLUSIVE.** The largest steady sampling gap is 17,351,622 ns,
exceeding the unchanged 5,000,000 ns density limit. No read/sampling error or
record overflow occurred. This is complete raw capture with sparse-memory
eligibility, not a memory pass; no limit was relaxed.

Budget-only elapsed observations: last closed slot offset 156.080065458s, Go
test reports 156.36s, measured process launch-to-exit 156.630057s. The outer
script command took 178.503449865s including source archiving and bookkeeping.
These establish that the diagnostic fits its declared budgets, not relative
performance. No p95, rate, paired ratio, cell/gate verdict or campaign PASS was
computed. `capture/diagnostic.json` says
`DIAGNOSTIC_CAPTURE_COMPLETE_NO_PERFORMANCE_VERDICT`; no `derived.json` exists.
No port-preflight row, second row, separate first child, retry or matrix
continuation ran. No containers were started.

The host was unpinned with CPU IDs 0–31 available. Row load was
17.73/11.45/6.94 before and 3.81/7.94/6.30 afterward. The readable ancestor
`/sys/fs/cgroup/user.slice/cpu.max` was `max 100000`; deeper process-controller
files were unavailable, so a tighter effective quota is not established. All
available and failed quota reads are retained in the manifest. GOMAXPROCS=8
was verified by the harness. No other benchmark was run concurrently.

No failure stack path was triggered. The implementation bounds the Go
all-goroutine stack to 2MiB with truncation metadata, and a native gdb attempt
to 3s/2MiB plus 1s pipe drainage. `gdb` and `pstack` were unavailable; no
installation/elevation occurred and no native backtrace is claimed. Actual
watchdog termination/native unwinding was not fault-injected in the one row;
the focused controls cover configuration, classification and output bounds.
A failed Go runtime/external kill can still prevent best-effort artifact writes.

## Hashes, durable audit and review handoff

The capture preserved source/binary/native hashes throughout execution.

- Fresh final binary: `3d57f97bc63e20ecbe60e801f938488f3652bbf58c6b81fd52051a6dce129cf2`.
- Native static archive: `562139fab7fec3ed44cdcb596276c1241d038ed95c0d34a165fa7d6d0f28ed7d`.
- Capture source archive: `290b6f8447574e556ad46937c714f5eed671c0d49fb3f87c510b4cd48d9e7e3e`.
- Capture source manifest: `adca2e53eb1bb13ec4d7fde5b3240ecfd03abb0217f16ae3ee83301e2e2f4a69`.
- Reducer/capture script: `9b97b6deeeca76b6b73e00eadd8fc5368fd5d0a7fc3e1a0ec32c1008d676f821`.
- Reducer test script: `891ac294c4c8cbcd193e4fc66f56a6cd1a31a4107875838cf907d34a0b155dcd`.
- Go capture source: `501e72782840c0b3f5b7ac435e8426e6cde231f6c5c8917a6b7ef5e2a51c1c74`.
- Raw diagnostic row: `f623e81fd3f4852321061b2d55902a31c38894414eb3ee5060fa2ad8b498b425`.
- Diagnostic completion JSON: `e24de22a8b749adbfab9d1c43f25f188592f6fff28a63ebc565ab964b75e9ce7`.
- Original failed row, unchanged: `0ad6fa39ac4ea91d600c4fd7863bbedcf4731f00d03f73c9d885d5f1b2835676`.

For repository delivery, Black formatted the two Python files after capture.
Their captured reducer hashes above remain the provenance for the replay. The
current formatting-only hashes are `1e827462b0882bb5d65a94301710b6f4d45be6137d98432dd09bc2a7a57df69f`
for `scripts/response-owner-measure.py` and
`3d64fb24f6aeb0bd359183a5cda00cb34c996ce4079b53c09cf4c9e7d1d7ef71` for
`scripts/test-response-owner-measure.py`; both Python syntax trees match their
captured versions. No measurement was rerun after formatting.

`native-provenance.json` verifies all 17 native/bridge source and lock paths
against the original capture, and the reused archive fingerprint. No native
build/tests were needed or run. `audit-diagnostic.py` verifies the raw result,
8/8 capture-ledger entries and all 118 files present in the original bundle
at amendment start, with zero mismatches. The final audit and
`bundle-manifest.json` seal the new evidence root; source scope and exact commands
are in the [durable structured audit](response-owner-diagnostic-2026-10-27.json).
The captured source archive remains immutable. After capture, only task result
documents and the two Black-formatted Python files changed. The Go capture
source, compiled binary, fixtures and native archive remain unchanged.
`git diff --check` passes.

The amendment touched the Go capture harness, Python capture/reducer and its
tests, plan/status/protocol/stopped-report prose, and this linked reference
plus its structured audit. The existing driver, effect ports, integration
witnesses, production Go, Rust, fixtures and lockfiles were untouched.

Parent acceptance is limited to the measurement harness and diagnostic
control/source/evidence. Matched-port/timing/raw-reproduction reviews and the
bounded-input-retention fix were accepted; Sol service-guard review 03 found no
blockers; the parent inspected six RSS-boundary guards and the strict-enclosure
edge fix; Luna's final diagnostic review found no blockers. The campaign remains
incomplete at its first row, with no comparative gates or performance verdict.
Exactly one replay is exhausted. The next measurement decision is pending
separate authorization. Phase 1 stays partial; Phase 2, process comparison,
native arm64 and activation gates remain open; bridge unset.
