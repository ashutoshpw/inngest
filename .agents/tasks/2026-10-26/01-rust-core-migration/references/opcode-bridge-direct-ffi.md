# Direct-buffer FFI experiment

**State:** accepted direct-buffer experiment; independent Luna safety and evidence reviews and Sol fairness review found no blockers. Representative screen passes 2/6 unchanged, uncalibrated cells; stop full-matrix expansion. Phase 2 remains open and bridge choice unset.
**Date:** 2026-10-26
**Worktree:** `/home/ashutosh/.t3/worktrees/inngest/t3-0eaf46e0`
**Branch/base:** `t3/direct-buffer-ffi`, `dfe2fd0663b9cf7e0675e965977a3300a5da01c4`.
Clean at `a4592cde49cab4c4a1afc5488cf39345514d5ed7` with no unique commits before a clean fast-forward. The prior worktree is untouched.

## Scope and test authoring gate (before edits)

Read-only discovery covered root AGENTS.md, agent-tasks and audit-test-cases skills and audit workflow, the existing plan/status and bridge contract/implementation/measurements/results, kernel `project`, FFI entrypoint/panic containment, Go adapter/semantic validator, executor oracle/materialization and measurement harness/tests. The parent explicitly authorized implementation and tests after independent evidence review; no renewed approval or delegation is required.

1. **Contract:** the new ABI rejects invalid bounds, null/misaligned pointers, overflow and overlapping regions; only its declared result prefix is initialized on success. The real Go Cgo adapter preserves all literal original-index/flag results and full payload identity from the independent 27-case/all-17-tag oracle, including concurrent calls.
2. **Credible regressions:** confusing word and byte capacities, constructing a slice over uninitialized output, overwriting canaries, reading null empty input, failing to reject overlap/unknown tags, mispartitioning native result arrays, dropping duplicate positions, or leaking results on errors/cancellation.
3. **Existing gap/primary owners:** existing framed-entry Rust tests cannot reach the new native-value ABI. Add ABI regression cases there. Extend the existing Go fixture and concurrent differential tests to run both real Cgo adapters; do not duplicate oracle fixtures/classification or repeat process lifecycle campaigns. Extend adapter input/cancellation cases at its real public boundary. The existing harness preflight compares actual groups/IDs/flags outside timing and its child-selection tests can cover the added lane.
4. **Seams:** one new C ABI entrypoint and one experiment Go adapter are required by the measurement caller. No redundant export, injection hook, test-only production wrapper, or runtime path is needed.

## ABI reasoning

The single new C symbol is:

```c
uint32_t opcode_project_v1(const uint8_t *tags, uint32_t tag_count,
                           uint32_t *output, uint32_t output_capacity);
```

The symbol versions the ABI and reuses tag-set v1 (0..16), 32-bit original indices, result bits 1 (history) and 2 (plan capability), the 65,536-opcode limit, and existing numeric error codes. All values are native fixed-width scalars; this is an in-process ABI, with no endian codec, wire frame, header parsing, handshake or request-ID round trip. Go still assigns/validates caller-local IDs for consistent diagnostics and equivalent FFI timing bookkeeping. There is one representation variant, exposed through decoded and full-response harness lanes.

Capacity is **in `uint32_t` words**, minimum 4, maximum `4 + 2*65,536`. Successful output is four words `[priority_count, other_count, non_lazy_count, flags]`, followed by priority, other, and non-lazy original indices. The groups partition n positions and non-lazy positions number at most n, so Go allocates exactly the worst-case `4 + 2*n` words (at most 524,304 bytes). Only `4 + sum(counts)` words are initialized on success; the suffix is untouched. Nonzero status always means no usable output. Ordinary validation/allocation/capacity failures write nothing; callers must also discard output on panic status rather than depend on transactional writes during an unexpected unwind.

Rust checks count/capacity bounds, nonempty input null, output null/alignment, checked word-to-byte multiplication, checked address-end addition, and overlap of the entire output capacity with the input before constructing an input slice. Empty input may be null and creates no raw slice. Numeric checks cannot prove allocation provenance/liveness or synchronization: a passing nonempty input must be readable/initialized in one allocation, and output must be writable/aligned in one allocation with exclusive access throughout the synchronous call. Neither pointer may be retained by Rust; neither may be changed concurrently by Go. Go passes only scalar buffers through Cgo, with `KeepAlive` through native return.

Rust converts checked bytes into a bounded internal tag vector with `try_reserve_exact`, calls the existing unchanged `opcode_kernel::project`, checks the total result word count, then raw-copies initialized header/Vec prefixes. It never creates a slice over caller output, so uninitialized writable storage is valid. Internal Rust vectors remain Rust-owned and are freed before return; there is no allocation-ownership transfer. Existing `contain_ffi_panic` and panic-payload-drop containment wrap the new entrypoint; the framed export and process paths are unchanged. OOM abort, invalid-pointer UB and native hangs are not made recoverable by panic containment.

Go checks input IDs/tags/count, checks cancellation before/after native execution, validates returned counts/reserved flags before slicing, then calls the unchanged `ValidateProjection`. It enforces complete unique coverage, original-position bounds, stable group order/membership, exact non-lazy order and flag semantics. The three returned views have capacities capped at their ends to prevent appends overwriting adjacent groups. They refer only to Go-owned storage. Full opcode payloads remain in Go; the harness materializes groups, duplicate/empty non-lazy IDs and flags by original index just as the existing framed/process lanes do. Negotiation, metadata, coalescing eligibility/hash/IDs, persistence and effects remain Go-owned. No production runtime wiring or fallback/selection policy was added.

## Coverage and harness

- Three Rust ABI tests cover mixed literal native results, initially uninitialized storage, untouched suffix canaries, exact/short capacity, empty null input, invalid nonempty null input, null/misaligned output, excessive count/capacity, both address-overflow sides, both overlap edges, unknown tags and maximum count/index writes. Existing shared panic-containment tests remain the primary owner for panic/payload-destructor handling; no forced-panic export was introduced.
- Existing executor FFI fixture and eight-caller tests run both adapters through real Cgo. All 27 literal cases/all 17 tags are retained; the differential test also compares the exact materialized groups/IDs/flags with Go helpers and retains the opaque duplicate-ID payload-identity check. No fixture expectations were regenerated.
- Adapter tests reject cancellation/invalid IDs/unknown tags/oversized input with no usable projection and exercise the maximum count through real Cgo, including nil-context handling. Raw ABI malformed pointers stay in Rust tests, rather than exporting a Go test seam.
- Distinct lanes are `ffi_direct_decoded` and `ffi_direct_full_driver_response_json`; all six previous lanes remain available. Exact existing lane/workload/concurrency selectors work unchanged, including `small1,mixed8,large1000` and concurrency `1,8`. Direct lanes use the same tag conversion, validation and final materialization timing boundary, preflight-only oracle/reflection, fresh row processes and resource snapshots/sampling as the controls. Build tags and Cgo-disabled unavailable-lane reporting remain intact. Rust internal allocation activity still is not counted by Go `B/op`.

## Independent safety review

Independent Luna review found no blockers in the Rust direct ABI, Go adapter, or focused regression coverage. The review checked the Rust bounds/alignment/overflow/overlap contract and initialized-prefix writes; real Go Cgo parity for all 27 literal cases and 17 tags; semantic validation and payload identity; concurrent calls; and focused Go race checks. Three direct Rust ABI tests and the shared panic-containment test passed. Go race checks passed for maximum input, the fixture, and concurrent calls. Go race instrumentation does not cover Rust raw-pointer memory accesses; Miri was unavailable.

## Accepted P2 measurement-fairness finding

Independent Sol measurement review found that direct FFI samples inherited the framed lanes' `ffiRequestID.Add(1)` inside the timed worker loop. Direct request IDs are caller-local diagnostics and never cross the native-value ABI, so eight concurrent callers needlessly contended on that shared atomic. The timed direct path now derives each ID from its already unique sample job index: `uint64(index) + uint64(4+concurrency)`. The preflight IDs remain 1/2, first request remains 3, and warmup remains 4 through `3+concurrency`; thus 16-sample smoke IDs are 5–20 at concurrency 1 and 12–27 at concurrency 8. They are nonzero, unique, and continue after warmup.

Framed FFI retains the atomic allocator because its request ID is encoded into the framed request. Process and Go-helper lanes remain unchanged. The serial benchmark was inspected: it already assigns FFI IDs as `uint64(index)+3` after preflight and has no shared atomic, so no source change was needed there. No source-shape or mirror test was added; existing adapter validation and the measurement harness's preflight output oracle remain the behavioral checks.

Focused follow-up checks used the existing x86_64 release artifacts and did not rebuild Rust. From this worktree, with Go 1.26.4 and cache-backed temporary directories:

```sh
export GOTOOLCHAIN=local GOMAXPROCS=2 CARGO_BUILD_JOBS=2
export GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-direct-ffi
export TMPDIR=/home/ashutosh/.cache/go-tmp-inngest-direct-ffi
export OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-0eaf46e0/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker
GO_DIRECT_FFI=/home/ashutosh/.cache/go1.26.4/go/bin/go

CGO_ENABLED=1 "$GO_DIRECT_FFI" test -p=2 -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor -run '^(TestFFI|TestDirectFFI|TestOpcodeBridge)' -count=1
# PASS: internal/opcodespike and executor; includes real Cgo adapter correctness
# and the measurement lane child-selection test.

CGO_ENABLED=1 OPCODE_SPIKE_MEASURE=1 OPCODE_SPIKE_MEASURE_LANES=ffi_direct_decoded,ffi_direct_full_driver_response_json OPCODE_SPIKE_MEASURE_WORKLOADS=small1,mixed8,large1000 OPCODE_SPIKE_MEASURE_CONCURRENCIES=1,8 OPCODE_SPIKE_MEASURE_SAMPLES=16 OPCODE_SPIKE_STARTUP_SAMPLES=1 OPCODE_SPIKE_MEASURE_SEED=26011 OPCODE_SPIKE_MEASURE_OUTPUT=/home/ashutosh/.cache/go-tmp-inngest-direct-ffi/direct-ffi-fairness-smoke.json "$GO_DIRECT_FFI" test -p=2 -tags opcode_bridge_spike,opcode_bridge_ffi ./pkg/execution/executor -run '^TestOpcodeBridgeMeasurementHarness$' -count=1 -v
# PASS: exact Cartesian selection of 12 direct-lane/workload/concurrency rows,
# 16 samples each; no worker processes. This is correctness/selector smoke only.

CGO_ENABLED=1 OPCODE_SPIKE_MEASURE_LANES=ffi_direct_decoded,ffi_direct_full_driver_response_json OPCODE_SPIKE_MEASURE_WORKLOADS=small1,mixed8,large1000 "$GO_DIRECT_FFI" test -p=2 -tags opcode_bridge_spike,opcode_bridge_ffi ./pkg/execution/executor -run '^$' -bench '^BenchmarkOpcodeBridgeSpikeSmoke$' -benchmem -benchtime=1x -count=1 > /home/ashutosh/.cache/go-tmp-inngest-direct-ffi/direct-ffi-fairness-benchmark-smoke.log
# PASS: six selected direct-lane/workload serial cases, one iteration each.
```

The first command passed (`internal/opcodespike` 0.035s; executor 0.625s). The bounded harness passed in 0.590s and its JSON selection was checked for all 12 exact tuples, 16 samples each, and zero worker processes. The serial benchmark passed in 0.058s; all six exact cases ran once. No ratios, thresholds, or performance conclusions were computed. Existing baseline result artifacts were not overwritten.

SHA-256: updated measurement source `pkg/execution/executor/opcode_bridge_measurement_test.go` `bf465cb0d7e99223e5d4ffc059d586a96d2eea69e0163fed3a45b10351cc88f6`; smoke JSON `cf0b1e274af8d0e188eb98842c5e81b8f4178eb500bfb902641407253c62f776`; serial smoke log `848bc5f21e523a7bae0e345149e31b7b395a22ef132271e11b8c0db2ffd28ca6`. Rust was not rebuilt; the existing archive and worker were `0f1c0cfc68b2fd697dce178779b9fec28773f0431d10aa0e04c9d04dfcb5eb63` and `f3ba9b27d2ae0d6720757df5decbb89016d0cfed41396a236f0a18a84ad22a79`, respectively.

## Validation

Validation uses Go `/home/ashutosh/.cache/go1.26.4/go/bin/go` (1.26.4), Rust 1.94.0, Linux/amd64 (`uname -m`: `x86_64`), `GOTOOLCHAIN=local`, `GOMAXPROCS=2`, `GOTMPDIR=TMPDIR=/home/ashutosh/.cache/go-tmp-inngest-direct-ffi`, Go `-p=2` and `CARGO_BUILD_JOBS=2`. The nearly full `/tmp` was not used for build/test scratch. Commands run from this worktree:

```sh
export GOTOOLCHAIN=local GOMAXPROCS=2 CARGO_BUILD_JOBS=2
export GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-direct-ffi
export TMPDIR=/home/ashutosh/.cache/go-tmp-inngest-direct-ffi
export OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-0eaf46e0/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker
GO_DIRECT_FFI=/home/ashutosh/.cache/go1.26.4/go/bin/go

cargo +1.94.0 fmt --manifest-path rust/Cargo.toml --all
cargo +1.94.0 test --manifest-path rust/Cargo.toml --workspace --locked
# PASS: 15 bridge tests (including 3 new direct ABI tests), 4 unchanged kernel tests.
cargo +1.94.0 clippy --manifest-path rust/Cargo.toml --workspace --all-targets --locked -- -D warnings
# PASS.
cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --lib --bin opcode-worker --release --target x86_64-unknown-linux-gnu --locked
# PASS: release archive and worker used by all following real Cgo/process checks.
cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --lib --release --target aarch64-unknown-linux-gnu --locked
# PASS: archive cross-build only, no native arm64 execution or Cgo final link.
cargo +1.94.0 fmt --manifest-path rust/Cargo.toml --all -- --check
# PASS.

CGO_ENABLED=1 "$GO_DIRECT_FFI" test -p=2 -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor -run '^(TestFFI|TestDirectFFI|TestOpcodeBridge)' -count=1
# PASS: internal 0.034s; executor 0.646s. Both real Cgo fixture paths,
# eight concurrent calls, process oracle/seeded controls and harness helper checks.
CGO_ENABLED=0 "$GO_DIRECT_FFI" test -p=2 -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor -run '^(TestTagsFromOpcodesRejectsInvalidExperimentInputsBeforeBridge|TestResultValidatorAcceptsExactCoverageAndRejectsMalformedAnswers|TestOpcodeBridgeSpike|TestOpcodeBridgeMeasurementLaneChildSelection)' -count=1
# PASS: internal 0.021s; executor 0.337s. Both tags with Cgo off use the
# process/Go lanes and compile the FFI-unavailable path, with no Rust static link.
CGO_ENABLED=0 "$GO_DIRECT_FFI" test -p=2 ./pkg/execution/executor -run '^TestOpcodePlanCompatibilityFixture$' -count=1
# PASS: executor 0.030s; default build has no experiment/Cgo dependency.
```

```sh
CGO_ENABLED=1 "$GO_DIRECT_FFI" test -p=2 -race -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor -run '^(TestFFI|TestDirectFFI|TestOpcodeBridgeFFI|TestOpcodeBridgeMeasurementLaneChildSelection)' -count=1
# PASS: internal 2.252s; executor 15.653s. Both adapters' fixture/concurrent
# calls, cancellation/input boundaries and real isolated child lane checks.

CGO_ENABLED=1 OPCODE_SPIKE_MEASURE=1 OPCODE_SPIKE_MEASURE_LANES=go_helper_decoded,process_decoded,ffi_decoded,ffi_direct_decoded OPCODE_SPIKE_MEASURE_WORKLOADS=small1,mixed8,large1000 OPCODE_SPIKE_MEASURE_CONCURRENCIES=1,8 OPCODE_SPIKE_MEASURE_SAMPLES=16 OPCODE_SPIKE_STARTUP_SAMPLES=1 OPCODE_SPIKE_MEASURE_SEED=2601 OPCODE_SPIKE_MEASURE_OUTPUT=/home/ashutosh/.cache/go-tmp-inngest-direct-ffi/decoded-smoke.json "$GO_DIRECT_FFI" test -p=2 -tags opcode_bridge_spike,opcode_bridge_ffi ./pkg/execution/executor -run '^TestOpcodeBridgeMeasurementHarness$' -count=1 -v
# PASS: executor 1.055s; 24 unique rows (4 decoded lanes x 3 workloads x
# concurrency 1/8), 16 calls per row, 3 one-sample process startup groups.
CGO_ENABLED=1 "$GO_DIRECT_FFI" test -p=2 -tags opcode_bridge_spike,opcode_bridge_ffi ./pkg/execution/executor -run '^$' -bench '^BenchmarkOpcodeBridgeSpikeSmoke$' -benchmem -benchtime=1x -count=1 > /home/ashutosh/.cache/go-tmp-inngest-direct-ffi/benchmark-smoke.log
# PASS: 24 serial cases (8 decoded/full-response lanes x 3 workloads),
# exactly one iteration each. No stable timing or allocation comparison inferred.
```

A separate JSON smoke check verified the complete exact 24-row Cartesian selection, 16 samples each, process worker counts equal caller concurrency and other lanes have zero workers, correct RSS scope, all five resource stages, 10ms RSS interval and separate first-request/startup metadata. No ratios or threshold passes were computed. Benchmark log validation confirmed all 24 cases ran once and passed.

Temporary local smoke artifacts (not committed campaign evidence):

- `/home/ashutosh/.cache/go-tmp-inngest-direct-ffi/decoded-smoke.json`, SHA-256 `db198114918d34bfbe28e1048a11f84654e4f38eed264b6d9f4fc8af6ac6aba9`.
- `/home/ashutosh/.cache/go-tmp-inngest-direct-ffi/benchmark-smoke.log`, SHA-256 `80ab74315d0cd80e7f4cef301aa008fc215ced65b8bf75361daed08f6a4af37d`.

Go formatting, Rust fmt check and `git diff --check` passed. `scripts/check-plan-status.sh` exited 1 solely for the existing `Implemented — pending staging review` status in `docs/plans/run-ai-debugging.md`; that file is unchanged. `cargo +1.94.0 miri --version` reports `the 'miri' component ... is not available for the '1.94.0-x86_64-unknown-linux-gnu' toolchain`. Miri was not run. No native arm64 runtime or arm64 final GNU Cgo link was run; cross-building an archive is not either gate. Existing result artifacts are immutable. This increment does not accept performance evidence or change proposed thresholds (p95 1.10x / throughput 0.90x / RSS 1.20x, still uncalibrated). Broad Phase 1 remains partial, Phase 2 open, bridge choice unset; native arm64 execution and final GNU Cgo link/runtime gates remain open.


## Changed files and review handoff

- `rust/crates/opcode-bridge/src/ffi.rs`: one native-value entrypoint, reused panic containment, and three ABI regression tests. Kernel and framed/process logic are unchanged.
- `internal/opcodespike/ffi_linux.go`: matching C declaration and bounded real Go adapter with full semantic validation.
- `internal/opcodespike/ffi_linux_test.go`: both adapters' cancellation checks plus direct input/max-count coverage.
- `pkg/execution/executor/opcode_bridge_ffi_test.go`: existing fixture/concurrent tests exercise both adapters; materialized output equality is explicit.
- `pkg/execution/executor/opcode_bridge_measurement_test.go`: distinct direct decoded/full-response lanes with the same timing/resource/preflight boundary and preserved controls.
- `pkg/execution/executor/opcode_bridge_measurement_ffi_on_test.go` and `opcode_bridge_measurement_ffi_off_test.go`: existing tagged harness dispatch supports the direct representation and keeps Cgo-disabled isolation.
- This reference plus existing `plan.md` and `status.md`: scoped progress, design, gate, commands, results and pending review. Prior result/measurement artifacts are unchanged.

Independent reviews accepted this experiment and its representative screening without blockers. The direct screen passes only 2/6 cells under unchanged, uncalibrated criteria; stop full-matrix expansion and keep the result scoped to this boundary. A larger-boundary read-only investigation remains separate and pending; this experiment does not establish broad Rust infeasibility. No production runtime wiring or bridge selection is added. Smoke timings are correctness/selector checks, not improvement or selection proof.

Retained limits: synchronous FFI supports pre/post context checks, not hard in-flight cancellation, native fault/hang recovery or ownership transfer. No forced hang/abort or deterministic late-cancellation injection was performed. Go race validation does not instrument Rust raw-pointer memory accesses. Rust still allocates bounded internal vectors, and Go allocation metrics exclude Rust allocation activity. Miri/native arm64/final arm64 GNU Cgo link/runtime validation remain open. Broader platforms and full migration/rollout gates remain as previously recorded.
