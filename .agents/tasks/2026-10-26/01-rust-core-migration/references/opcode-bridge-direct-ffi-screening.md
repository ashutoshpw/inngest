# Direct-buffer FFI representative amd64 screening

**Date:** 2026-10-26. **State:** independent safety, fairness, and evidence reviews accepted with no blockers. **Decision:** direct screening gate fails (2/6 cells); stop full-matrix expansion. Broad Phase 1 remains partial, Phase 2 open, production bridge unset; no production wiring.

## Independent read-only fairness verdict

Accepted with no blocker. Reviewed the actual uncommitted final diff against `dfe2fd0663b9cf7e0675e965977a3300a5da01c4` on `t3/direct-buffer-ffi` in `/home/ashutosh/.t3/worktrees/inngest/t3-0eaf46e0`. The accepted independent Luna safety review checked the ABI bounds/alignment/overflow/overlap and initialized-prefix contract, direct Rust tests, Go Cgo parity and semantic validation, concurrent calls, and focused Go race checks. This verdict concerns the fairness fix, separately from the measurement evidence below.

In `measureOpcodeSpikeLane`, direct IDs use each buffered-channel job index exactly once: `uint64(index)+uint64(4+concurrency)`. For 5,000 samples, this is 5–5004 at C1 and 12–5011 at C8. Preflight reserves 1/2, first request 3, warmup 4..3+C; the ranges are nonzero, disjoint and bounded. Different rows have isolated processes; diagnostics do not require globally unique IDs. Direct lanes skip both atomic initialization and `Add`. Framed FFI retains its allocator; process and Go dispatch are unchanged. The serial benchmark already uses index+3 after preflight 1/2, with no shared atomic.

The timing boundary remains before caller ID assignment through final groups/IDs/flags, including tag conversion, native storage allocation, Cgo, Rust projection/allocation, complete semantic validation and Go materialization. Oracle/reflection preflight remains untimed. No required direct cost was removed and no new direct shared contention was found. No source-shape test or repair edit was made. The test-audit review retained existing behavioral boundary coverage; no tests were authored.

## Accepted independent reviews and disposition

Independent Sol fairness review identified that timed direct FFI lanes still used the framed path's shared atomic request-ID allocator. Luna removed that direct-only contention by deriving IDs from each unique sample job index while keeping ID assignment and all required direct costs inside the timed boundary. Sol independently accepted the fix and ran this representative screen.

The latest independent Luna evidence review, rust-direct-ffi-screening-review-01, found no blockers. It recalculated all 54 seed-matched paired ratios and 18 summary cells; verified all 63 cached artifact sizes/hashes, source and binary manifests/fingerprints, and serial summaries; and confirmed the 2/6 direct result supports stopping full-matrix expansion.

This is a narrow Linux amd64 result for the direct-buffer boundary. It does not establish broad Rust infeasibility or select a production bridge. Keep Phase 2 open and bridge choice unset. A larger-boundary read-only investigation remains separate and pending.

## Authored screening evidence

Ran exclusively and sequentially, without delegation, source edits, affinity changes, host tuning, installation or unrelated process termination. Exactly three latency commands used seeds 26011/26012/26013, GOMAXPROCS=8, the four decoded lanes, small1/mixed8/large1000, C1/C8, 5,000 steady samples, and 10 startup samples. Exactly twelve serial commands selected one workload × one lane each, GOMAXPROCS=1, 500ms × five repetitions, CPU=1. All 15 screening commands exited 0; all seven version/build commands also exited 0. Exact argv, environment overrides, start/end UTC, wall time, exits, logs and SHA-256 are in the JSON.

Selection/resource audit passed: 72 unique rows and fresh host PIDs, 360,000 steady calls, 360 snapshots in baseline/ready/warmup/end/after_close order, 18 process rows, 81 worker PID absence records after close, and zero workers for Go/FFI. Worker PID sets, resource scopes, aggregate sums, warmup counts, READY/first-request metadata and host FD/goroutine return-to-baseline checks match. Nine ordered startup groups contain 90 fresh size-one-pool sample pairs. All 60 serial repetitions are present, with no skip or failure. Pairing yields 54 per-run candidate/Go comparisons and 18 three-run summary cells; no extra selection was run.

Thresholds are unchanged and explicitly **uncalibrated**: candidate/Go p95 ≤ 1.10, closed-loop throughput ≥ 0.90, sampled peak RSS ≤ 1.20. The direct gate requires all six cells to pass all three median paired metrics, with each metric passing at least two of three runs. Ratios pair identical seed/workload/concurrency; they are medians of ratios, not ratios of pooled durations or median raw metrics.

### Paired median ratios

Each value is median paired ratio followed by individual run passes out of three. All routes are compared to `go_helper_decoded`.

| Lane | Workload | C | p95 | Throughput | Peak RSS | Cell |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| process_decoded | small1 | 1 | 121.7048 (0/3) | 0.0112 (0/3) | 1.1036 (3/3) | FAIL |
| process_decoded | small1 | 8 | 137.9195 (0/3) | 0.0710 (0/3) | 1.2373 (0/3) | FAIL |
| process_decoded | mixed8 | 1 | 35.6542 (0/3) | 0.0317 (0/3) | 1.1022 (3/3) | FAIL |
| process_decoded | mixed8 | 8 | 43.9888 (0/3) | 0.0693 (0/3) | 1.2143 (0/3) | FAIL |
| process_decoded | large1000 | 1 | 1.5721 (0/3) | 0.2657 (0/3) | 1.0187 (3/3) | FAIL |
| process_decoded | large1000 | 8 | 0.7414 (3/3) | 0.8844 (1/3) | 1.1358 (3/3) | FAIL |
| ffi_decoded | small1 | 1 | 3.0048 (0/3) | 0.3390 (0/3) | 0.9886 (3/3) | FAIL |
| ffi_decoded | small1 | 8 | 3.7276 (0/3) | 0.8036 (0/3) | 0.9873 (3/3) | FAIL |
| ffi_decoded | mixed8 | 1 | 1.7459 (0/3) | 0.6049 (0/3) | 1.0048 (3/3) | FAIL |
| ffi_decoded | mixed8 | 8 | 1.5859 (0/3) | 0.9381 (3/3) | 1.0056 (3/3) | FAIL |
| ffi_decoded | large1000 | 1 | 0.8810 (3/3) | 0.5403 (0/3) | 1.0197 (3/3) | FAIL |
| ffi_decoded | large1000 | 8 | 0.7649 (3/3) | 1.5858 (3/3) | 1.0259 (3/3) | PASS |
| ffi_direct_decoded | small1 | 1 | 1.8143 (0/3) | 0.5212 (0/3) | 0.9898 (3/3) | FAIL |
| ffi_direct_decoded | small1 | 8 | 2.1111 (0/3) | 0.8565 (0/3) | 0.9953 (3/3) | FAIL |
| ffi_direct_decoded | mixed8 | 1 | 1.0416 (2/3) | 0.8965 (1/3) | 0.9967 (3/3) | FAIL |
| ffi_direct_decoded | mixed8 | 8 | 0.8889 (3/3) | 1.0062 (2/3) | 0.9903 (3/3) | PASS |
| ffi_direct_decoded | large1000 | 1 | 0.7071 (3/3) | 0.7187 (1/3) | 1.0069 (3/3) | FAIL |
| ffi_direct_decoded | large1000 | 8 | 0.5496 (3/3) | 1.7300 (3/3) | 1.0258 (3/3) | PASS |

### Direct per-run ratios

| Workload | C | Seed | p95 | Throughput | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| small1 | 1 | 26011 | 1.805687 | 0.575404 | 0.966157 |
| small1 | 1 | 26012 | 1.814286 | 0.521158 | 1.001934 |
| small1 | 1 | 26013 | 3.546154 | 0.389739 | 0.989764 |
| small1 | 8 | 26011 | 2.784232 | 0.856464 | 0.982422 |
| small1 | 8 | 26012 | 1.697674 | 0.865739 | 1.027468 |
| small1 | 8 | 26013 | 2.111111 | 0.440222 | 0.995289 |
| mixed8 | 1 | 26011 | 0.910581 | 0.973910 | 0.998374 |
| mixed8 | 1 | 26012 | 1.145191 | 0.763532 | 0.980624 |
| mixed8 | 1 | 26013 | 1.041609 | 0.896548 | 0.996730 |
| mixed8 | 8 | 26011 | 0.888889 | 1.006172 | 0.975532 |
| mixed8 | 8 | 26012 | 0.901460 | 1.023158 | 0.996983 |
| mixed8 | 8 | 26013 | 0.779271 | 0.131625 | 0.990334 |
| large1000 | 1 | 26011 | 0.707143 | 0.718700 | 1.013471 |
| large1000 | 1 | 26012 | 0.640189 | 0.959666 | 0.837040 |
| large1000 | 1 | 26013 | 0.816143 | 0.649750 | 1.006857 |
| large1000 | 8 | 26011 | 0.546288 | 1.730005 | 1.147587 |
| large1000 | 8 | 26012 | 0.549583 | 1.592972 | 1.011879 |
| large1000 | 8 | 26013 | 0.634355 | 1.913216 | 1.025840 |

The two small1 cells fail p95 and throughput in all three runs, a repeated narrow negative. C1 mixed8 throughput (median 0.896548, 1/3 passes) and C1 large1000 throughput (0.718700, 1/3 passes) also fail. Crossings in mixed8 C1 and large1000 C1 are inconclusive as stable speed claims on this shared, unpinned host; no causal attribution to order or load is established. Mixed8 C8 passes the prescribed rule but throughput falls to 0.131625 in seed 26013, despite passing p95; that variability further limits interpretation. Large1000 C8 passes all metrics in all runs. Passing selected cells does not satisfy the six-cell gate.

**Stop expansion:** this screening does not support the full matrix. A larger useful work boundary or redesign is required before a separately authorized experiment; this is not a production bridge choice. No full matrix, native arm64 execution, final arm64 GNU Cgo link, production wiring, commit, push or PR was performed.

### Serial support

Arithmetic mean ns/op and Go allocation counts across five repetitions. Serial throughput ratio is mean Go ns/op / mean candidate ns/op and is supporting only; it does not replace the latency harness gate. Go B/op and allocs/op exclude Rust allocator activity.

| Lane | Workload | Mean ns/op | Range ns/op | B/op | allocs/op | Serial throughput / Go |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| go_helper_decoded | small1 | 111.28 | 110.3–113.0 | 24.0 | 2.0 | 1.0000 |
| process_decoded | small1 | 15967.40 | 15173.0–17183.0 | 1452.0 | 24.0 | 0.0070 |
| ffi_decoded | small1 | 454.66 | 444.8–479.3 | 120.0 | 8.0 | 0.2448 |
| ffi_direct_decoded | small1 | 278.74 | 272.3–290.4 | 49.0 | 4.0 | 0.3992 |
| go_helper_decoded | mixed8 | 446.76 | 431.9–470.1 | 368.0 | 9.0 | 1.0000 |
| process_decoded | mixed8 | 14003.00 | 13534.0–14497.0 | 1720.0 | 26.0 | 0.0319 |
| ffi_decoded | mixed8 | 771.28 | 747.4–816.3 | 408.0 | 10.0 | 0.5792 |
| ffi_direct_decoded | mixed8 | 508.24 | 492.9–518.9 | 256.0 | 5.0 | 0.8790 |
| go_helper_decoded | large1000 | 27371.60 | 25625.0–29035.0 | 53808.0 | 29.0 | 1.0000 |
| process_decoded | large1000 | 64346.20 | 57568.0–76251.0 | 47416.0 | 29.0 | 0.4254 |
| ffi_decoded | large1000 | 37499.00 | 36397.0–38872.0 | 46084.0 | 13.0 | 0.7299 |
| ffi_direct_decoded | large1000 | 30445.40 | 29346.0–31487.0 | 36224.0 | 7.0 | 0.8990 |

## Environment and reproducibility

Linux x86_64, AMD Ryzen 9 5950X (16 cores/32 logical CPUs); affinity CPUs 0–31 before/after, unchanged. Go 1.26.4 from `/home/ashutosh/.cache/go1.26.4/go/bin/go`, Rust/Cargo 1.94.0, Cgo enabled, GNU cc 11.4.0. GOTOOLCHAIN=local; compilation GOMAXPROCS=2, Go -p=2, CARGO_BUILD_JOBS=2. GOTMPDIR/TMPDIR use the unique cache directory’s `tmp/`; /tmp remained 99% full and was not scratch. Rust release `--locked --target x86_64-unknown-linux-gnu` build completed successfully with current cached release artifacts (Cargo reported 0.04s); the real Cgo executor test binary was compiled with `opcode_bridge_spike,opcode_bridge_ffi`. No claim of a clean Cargo build.

- host-before-build, 2026-10-26T21:30:49.331183+00:00: load 5.41 5.19 6.27 2/13323 3463336.

- host-before-measurement, 2026-10-26T21:30:56.111674+00:00: load 5.03 5.12 6.24 3/13301 3464418.

- host-after-measurement, 2026-10-26T21:31:57.857745+00:00: load 4.20 4.84 6.07 2/13321 3477412.

Unrelated browser/server/worker activity was present. The focused process listing before and after found no separate executor benchmark campaign. No lock or host isolation against all possible external work is claimed. Full CPU, memory, process-count and affinity observations are retained in JSON.

| Fingerprint | SHA-256 |
| --- | --- |
| opcode-worker | `f3ba9b27d2ae0d6720757df5decbb89016d0cfed41396a236f0a18a84ad22a79` |
| libopcode_bridge.a | `0f1c0cfc68b2fd697dce178779b9fec28773f0431d10aa0e04c9d04dfcb5eb63` |
| executor.test | `1d525924a5516693da96464feec0386a87ab97e5b1eab80f871a14511d58cf48` |
| manifestBeforeSHA256 | `0eb6f44ea47dc3b8c3a16b5f0ae3f480f9ec185447598e82d38dccd3c6f6b958` |
| manifestAfterSHA256 | `0eb6f44ea47dc3b8c3a16b5f0ae3f480f9ec185447598e82d38dccd3c6f6b958` |
| sourcePatchBeforeSHA256 | `d68624c19624054a9776894e7c073725fb12cfc3d5458b5619389f080f3dc3e8` |
| sourcePatchAfterSHA256 | `d68624c19624054a9776894e7c073725fb12cfc3d5458b5619389f080f3dc3e8` |
| fullUncommittedPatchBeforeSHA256 | `1d28970a4616eb2e4a508fc3abb7ab024de816bfaeeb30da4576cb2e20204170` |

The full non-documentation source manifest covers 12,751 tracked/unignored files, including vendor and fixtures, excluding `.agents/`, `docs/`, `.md` and `.org`. Full manifest and patch bytes are retained in cache; JSON includes hashes, cache artifact sizes/hashes and a focused experiment-file manifest. Source patch excludes `.agents/` and `docs/`. The full initial uncommitted patch includes existing tracked documentation; the pre-existing untracked direct-FFI reference is separately fingerprinted in the prior-reference manifest. Source manifest, source patch and three binaries match before/after; final documentation-only handoff rechecks them. Original campaign/result artifacts are unchanged.

All 72 rows retain five resource stages. However **35/72 rows (11/18 direct rows) have only initial and final RSS observations**; they cannot establish memory stability. The 10ms sampler can miss short peaks and perturb timing. Process aggregate RSS includes host plus row workers and double-counts shared pages; Go/FFI RSS includes the isolated Go runtime and workload fixtures. Threads/RSS may remain retained after close. Startup groups run in fixed workload order with ten fresh pools each, have no sampled memory and are not cold-cache comparisons.

**Schema 3 reproducibility limit:** exports preserve p50/p95/p99 row summaries, not individual request durations. Percentiles cannot be independently reconstructed from these artifacts. The three seeded reports are preserved verbatim as nested JSON objects, including actual shuffled row order and resource records; the raw cache JSON/log hashes are recorded. This limitation is unchanged.

## Command ledger and artifacts

Raw execution/cache directory: `/home/ashutosh/.cache/direct-ffi-screen-20261026-w_f3xfh6`. The runner and analysis scripts are cache artifacts, not production/test changes. Each command has a separate `.log` and `.exit`; command failure would stop the runner before later commands. The structured evidence contains the exact commands, not reconstructed shorthand.

| Command | Exit | Seconds |
| --- | ---: | ---: |
| go-version | 0 | 0.012 |
| rust-version | 0 | 0.049 |
| cargo-version | 0 | 0.021 |
| cc-version | 0 | 0.002 |
| go-env | 0 | 0.003 |
| build-rust | 0 | 0.053 |
| build-go | 0 | 5.699 |
| latency-26011 | 0 | 2.162 |
| latency-26012 | 0 | 2.380 |
| latency-26013 | 0 | 2.299 |
| bench-go_helper_decoded-small1 | 0 | 3.611 |
| bench-process_decoded-small1 | 0 | 4.892 |
| bench-ffi_decoded-small1 | 0 | 5.404 |
| bench-ffi_direct_decoded-small1 | 0 | 4.468 |
| bench-go_helper_decoded-mixed8 | 0 | 5.355 |
| bench-process_decoded-mixed8 | 0 | 4.710 |
| bench-ffi_decoded-mixed8 | 0 | 3.349 |
| bench-ffi_direct_decoded-mixed8 | 0 | 3.856 |
| bench-go_helper_decoded-large1000 | 0 | 4.501 |
| bench-process_decoded-large1000 | 0 | 4.881 |
| bench-ffi_decoded-large1000 | 0 | 4.989 |
| bench-ffi_direct_decoded-large1000 | 0 | 4.663 |

- [Structured screening evidence](opcode-bridge-direct-ffi-screening.json): commands/statuses, raw hashes, versions, host/source fingerprints, 72 full schema-3 rows/resources, startup groups, all paired run/median data and 60 serial repetitions.
- [Paired run CSV](opcode-bridge-direct-ffi-screening-paired.csv): all 54 candidate/Go ratios with seed, order, p95, throughput, sampled RSS and threshold outcomes.
- [Serial repetition CSV](opcode-bridge-direct-ffi-screening-benchmarks.csv): all 60 iterations/ns/op/B/op/allocs records.
- [Direct implementation reference](opcode-bridge-direct-ffi.md): ABI contract, independent Luna safety review, focused Rust/Go correctness and race evidence, and retained platform/cancellation limits.

Documentation verification: JSON/CSV cardinality and hash assertions passed; git diff --check passed. scripts/check-plan-status.sh exited 1 only for the pre-existing invalid value in docs/plans/run-ai-debugging.md; no repair was made. Independent safety, fairness, and evidence reviews are accepted with no blockers. Stop full-matrix expansion; preserve all broader parity, platform, lifecycle, rollout, and retirement gates.
