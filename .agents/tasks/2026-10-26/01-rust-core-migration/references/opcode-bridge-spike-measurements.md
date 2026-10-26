# Opcode bridge spike measurement campaign

**State:** Linux/amd64 campaign and independent Sol evidence review accepted; Phase 2 remains open; bridge choice unset; native arm64 gates remain open.
**Date:** 2026-10-26
**Worktree/branch:** `/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7`, `t3/opcode-bridge-spike`
**Base:** `412b9af982ba41c7e06716c3c1a099e198f4147f`
**Prior accepted implementation snapshot file tree SHA-256:** `7e4cd586094cd5d8fbb2483674041a3332d42a347cfb1e3574ed31540c4317b4` (23 implementation, harness, manifest, and fixture files; excludes ignored `rust/target/` outputs)
**Prior snapshot tracked patch SHA-256:** `794f8871d881200d9ea558ff6499f486099ffa10416c29cdfaf78def14e3e957`
**Prior snapshot untracked manifest SHA-256:** `e647742ab718587a4fc60479dc80f24b9ac6b5e0447c9cba4b199d29d5d63eb0` (23 files, excluding this report)
**Prior snapshot worktree delta SHA-256:** `812af09c65da093372377c930b62446b5d3750839be14b3f6ca2d0cb73a6826b` (base + tracked patch + untracked manifest, excluding this report to avoid a self-referential hash)
**Campaign raw logs and schema-v3 result JSON:** retained in `/home/ashutosh/.cache/go-tmp-inngest-opcode-spike/measurement-capabilities`; all command and artifact hashes are in the [structured results](opcode-bridge-spike-results.json). The implementation snapshot hashes above predate measurement-only additions and do not identify the current harness.

## Review and campaign outcome

The parent accepted the implementation correctness reviews with no blockers.
Luna accepted the measurement harness: actual Go helper and bridge required
costs are measured, every latency row runs in a fresh test subprocess with its
own resources, and all 21 invocations were ready and executed. The accepted P2
interpretation limit is that startup groups run in fixed order before the
seeded steady-state row shuffle. Startup values are reported as ordered
observations, not randomized startup comparisons; no harness change was made
for this limitation. Sol independently checked the source snapshot, all 21
command outcomes, 288 rows across three seeds, 180 serial benchmark
repetitions, raw artifact hashes, and derived calculations; the review found
no blockers.

The complete Linux/amd64 campaign produced 288 latency rows across seeds
75501, 75502, and 75503, plus six 500-sample process startup groups per run and
18 serial benchmark rows with ten one-second repetitions each. All 21 commands
exited 0. The [human results report](opcode-bridge-spike-results.md),
[structured results and raw-file hashes](opcode-bridge-spike-results.json),
and CSV exports are the source of measurement outcomes. Thresholds remain
proposed and uncalibrated: p95 <= 1.10x Go, closed-loop throughput >= 0.90x Go,
and aggregate sampled peak RSS <= 1.20x Go. The measured per-route outcomes,
including failures, are reported without threshold changes. Phase 2 remains
in progress and bridge choice remains unset.

### Startup, first request, and steady requests

Schema version 3 separates `processStartup[].spawnThroughReady` and
`firstCompletedRequest` distributions. Each paired process startup sample
creates a fresh size-one pool through the actual READY handshake, completes
one request through the same decoded or full `DriverResponse` JSON path as the
steady lane, checks materialized equality after timing, and closes the pool.
The representative `small1`, `mixed8`, and `large1000` workloads are covered
when selected. Spawn/READY, first-request, and shutdown time are not merged.
These distributions have `rssScope=not_sampled`; zero RSS summary fields in
these records are not resource measurements.

Every isolated latency row also records its first completed request before
preflight; process rows separately record startup through all configured
workers READY. Go/FFI rows omit the inapplicable READY field. After preflight, one untimed request per configured pool slot warms every
worker before GC and steady measurement. `warmupRequestsExcluded` records
these calls plus preflight (excluding the separately recorded first request).
First-request, preflight/warmup, GC, startup, and close are excluded from steady latency and
closed-loop wall throughput. The first request is serial even in a concurrency
8 row; fresh-worker first-request distributions use size-one pools.

The accepted timing boundary remains before FFI-only caller request-ID
assignment and input decoding through final groups, non-lazy IDs, and flags.
It includes pool admission and transport. The Go baseline calls `opGroups`,
`NonLazyIDs`, and `hasPlanOp`; bridge lanes include tag conversion, codec,
transport, semantic validation, and index materialization. The copying/index
oracle is preflight only. Materialized equality is checked outside timing.

### Resource observations

Each lane/workload/concurrency row uses a fresh Go test subprocess. Resource
snapshots are emitted at `baseline`, `ready`, `warmup` (the real steady initial
observation after GC and measurement-buffer allocation), `end`, and
`after_close`. The baseline initializes runtime pipe polling and GC before
pool creation, so lazy runtime polling descriptors are not reported as leaks.

Each snapshot has the Go host PID, FD count, OS thread count, RSS, and Go
goroutine count, plus separate worker PID/FD/thread/RSS records. Rust std
workers have no Go goroutine field. `workerRSSBytes` includes only this row's
worker PIDs; `aggregateRSSBytes` adds the row's Go host. No other child process
or campaign parent RSS is included. After close, the same worker PIDs are
recorded with `state=absent_expected_after_close`; absent workers have no
fabricated resource counters. Unexpected process disappearance or unreadable
live counters fails the row, rather than silently becoming zero. Close errors,
workers still present, and host FD/goroutine counts that do not return to the
baseline within one second also fail the row. Runtime-retained threads/RSS
are reported without requiring them to shrink.

`sampledRSSPeaks` declares the 10 ms interval and observation count, and records
host, individual worker, worker-only aggregate, and host-plus-worker aggregate
RSS peaks. The aggregate peak is the largest sum in a single observation,
not the sum of independent process peaks. `/proc` files are read sequentially
within each observation; these are approximate simultaneous totals, not an
atomic kernel snapshot. Initial and final observations also participate in
peak tracking. Sampling can miss sub-10-ms peaks and can perturb latency;
summed process RSS double-counts shared pages. FD/thread/goroutine values are
stage snapshots, not continuously sampled maxima.

### Benchmark matrix and exact selection

`BenchmarkOpcodeBridgeSpikeSmoke` now covers all six Go/process/FFI decoded
and full-response JSON lanes for `small1`, `mixed8`, and `large1000`, with
allocation reporting and equality preflight outside timing. A missing FFI
build explicitly skips FFI benchmark subcases (`opcode_bridge_ffi` and Cgo
are both required); the latency report also names unavailable FFI lanes.
Requesting unavailable latency lanes fails rather than silently omitting them.

Allocation/throughput benchmarks are **serial**, with a size-one process pool.
Use the latency harness's concurrency 1/8 wall throughput for the matched
concurrent comparison. No `RunParallel` result is claimed. For campaign
isolation, the commands below select exactly one benchmark lane/workload per
fresh test process, using ten one-second repetitions. A convenience invocation
without selectors traverses all 18 benchmark cases in one host and is suitable
for smoke validation only.

The harness requires `OPCODE_SPIKE_MEASURE=1`, defaults to 10,000 individual
steady calls per row, and defaults startup samples to `min(samples, 500)`.
Controls are:

- `OPCODE_SPIKE_MEASURE_LANES`: exact comma-separated available lane names.
- `OPCODE_SPIKE_MEASURE_WORKLOADS`: exact comma-separated workload names;
  latency supports all eight, benchmarks accept the three representatives.
- `OPCODE_SPIKE_MEASURE_CONCURRENCIES`: exact selection from `1,8` (latency).
- `OPCODE_SPIKE_MEASURE_SEED`: optional signed integer seed for shuffling
  latency rows; the seed and actual ordered result list are exported.
- `OPCODE_SPIKE_MEASURE_SAMPLES`, `OPCODE_SPIKE_STARTUP_SAMPLES`, and
  `OPCODE_SPIKE_MEASURE_OUTPUT`: steady count, paired startup count, JSON path.

Partial names, whitespace, duplicates, empty entries, unknown names, and
unavailable lane requests are errors. Child row controls are the exact
`OPCODE_SPIKE_MEASURE_CHILD_LANE`, `_WORKLOAD`, and `_CONCURRENCY` keys;
children inherit the sample count and write to their parent-provided output.
The per-row timeout remains five minutes. Process pool size and queue bound
both equal caller concurrency (1 or 8). Each stage failure fails the test;
failed subprocess output is surfaced by the parent.

## Executed command bundle

The exact accepted command bundle is
`/home/ashutosh/.cache/go-tmp-inngest-opcode-spike/measurement-capabilities/campaign-commands.txt`
(SHA-256 `5107cc40543ef3f7a1b36a7a7bbac8c8a8efb652e5ea0c0ce949019b502f43ba`).
It ran three seeded full latency invocations (10,000 samples per row, 500
startup samples per group) followed by 18 sequential serial benchmark
invocations (six lanes × three representative workloads, ten one-second
repetitions per command). Exact command strings, individual exit codes and
wall durations, raw log hashes, result JSON hashes, and executable fingerprints
are recorded in the structured results artifact. No rebuild was needed: the
Go test executable and Rust worker/archive matched the accepted source snapshot.

## Prior accepted checks and unchanged artifact state

The parent accepted the implementation correctness reviews with no blockers,
including the final shutdown BYE race review. The fixes represented in this
snapshot cover uninitialized FFI output, bounded lifecycle cleanup/restart and
budgets, the shutdown race, and the reviewed measurement timing/oracle/Go
helper/row-isolation/RSS/FFI request-ID corrections.

Validation rerun on this worktree:

| Check | Result |
| --- | --- |
| `cargo +1.94.0 fmt --all -- --check` | Pass |
| Locked Rust workspace Clippy, all targets, `-D warnings` | Pass |
| Locked Rust workspace tests | Pass: 12 bridge, 4 kernel |
| Go 1.26.4 process path, `CGO_ENABLED=0`, spike tag | Pass |
| Go 1.26.4 process + FFI path, `CGO_ENABLED=1`, both spike tags | Pass |
| Go race checks, three repetitions, `GOMAXPROCS=1` | Pass |
| amd64 GNU release worker and FFI static library | Current; build commands passed |
| aarch64 GNU release FFI static library | Rebuilt; archive inspected |
| aarch64 musl release worker | Rebuilt; static AArch64 ELF inspected |
| Native arm64 execution / GNU Go Cgo final link | Not available; no native result claimed |

`aarch64-linux-gnu-gcc` and `qemu-aarch64` are absent. No system compiler was
installed and no emulator was used. Thus the two-architecture runtime gate
remains incomplete.

Current release artifact hashes:

| Artifact | SHA-256 |
| --- | --- |
| `rust/target/x86_64-unknown-linux-gnu/release/opcode-worker` | `ee0a3445b7ab90dbdc57e4bb1f375db087075134208e98ea113d4c80b87acb47` |
| `rust/target/x86_64-unknown-linux-gnu/release/libopcode_bridge.a` | `0fda795af636942e04e7f2be0f9988cdb7586b0921882f45ffcb5e81f66c5abe` |
| `rust/target/aarch64-unknown-linux-gnu/release/libopcode_bridge.a` | `baaed64c1ef85ce32b2ac40b872875c3087450e05b5886355207cd9ca33591ff` |
| `rust/target/aarch64-unknown-linux-musl/release/opcode-worker` | `a04c57b9502c35fdc332c36a1843db69c062374bc30c6dd6bf2ef494d7f2cc63` |

## Environment and reproducibility snapshot

The Rust commands used the checked-in `rust/rust-toolchain.toml`: Rust/Cargo
1.94.0, `x86_64-unknown-linux-gnu` host, release profile with panic unwind,
locked dependencies. Go tests used the explicit Go 1.26.4 binary at
`/home/ashutosh/.cache/go1.26.4/go/bin/go`, `GOTOOLCHAIN=local`, an empty
`GOFLAGS`/`GOEXPERIMENT`, and
`GOTMPDIR=TMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike`; no Go
temporary files were directed to the nearly full `/tmp` filesystem. Go Cgo
defaults were `CC=gcc`, `CGO_CFLAGS=-O2 -g`, and `CGO_LDFLAGS=-O2 -g`;
`gcc` was 11.4.0. Race validation set `GOMAXPROCS=1`.

| Resource | Snapshot |
| --- | --- |
| Host/kernel | Linux 5.15.0-160-generic, x86_64 |
| CPU | AMD Ryzen 9 5950X, 16 cores / 32 logical CPUs; `nproc=32` |
| Affinity | `Cpus_allowed_list=0-31`; effective cpuset `0-31` |
| CPU quota | No quota (`cpu.max=max` at the exposed `user.slice`); no throttling reported |
| Memory | Host `MemTotal=131806492 kB`; T3 agents ancestor limit `130674880512` bytes; current leaf limit is `max` |
| `/tmp` | 30 GiB filesystem, 331 MiB free at inspection (99% used) |
| Go temp filesystem | `/home` filesystem, 978 GiB free at inspection; Go temp used this cache path |
| Process limit | Current cgroup `pids.max=154325`; `ulimit -n=1048576` |

The machine snapshot is environment context, not a guarantee of repeatable performance. The completed campaign results and preflight load snapshot are recorded in the [results report](opcode-bridge-spike-results.md). The host was not CPU-pinned and the preflight load was elevated; three-run variance limits how strongly a median can support a route choice. CPU frequency, background host load, cache state, and sampled RSS can vary. Startup groups were ordered, and no cold-cache comparison was controlled.

The base commit is `412b9af982ba41c7e06716c3c1a099e198f4147f`. The prior snapshot
implementation-file-tree hash above covered the 23 relevant source/test/build
files and fixture, excluding generated/ignored `rust/target` outputs. The
experiment adds no production wiring or runtime source changes, and it leaves
bridge choice unset. PR delivery metadata is recorded in the task status file.

## Remaining gates

- Complete native Linux/arm64 process execution and the final GNU FFI Cgo link/runtime check; no arm64 performance result is claimed.
- Keep Phase 2 in progress with the bridge choice unset and broad Phase 1 partial. FFI hard cancellation is unavailable, and `CGO_ENABLED=0` process packaging versus FFI policy remains unresolved.
- If work continues, the next bounded experiment is a direct FFI caller-buffer path that avoids the wire-frame round trip; it does not select a bridge.
- Keep full parity, rollout, recovery, platform, cloud, and retirement gates unchanged.
