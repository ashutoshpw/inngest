# Opcode bridge spike implementation evidence

**State:** Phase 2 implementation in progress; bridge choice unset.
**Date:** 2026-10-26
**Worktree:** `/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7`
**Branch/base:** `t3/opcode-bridge-spike` at the accepted PR5 commit
`412b9af982ba41c7e06716c3c1a099e198f4147f`, the same local base as
`t3/opcode-grouping-oracle`.
**Review/delivery:** the parent accepted the implementation reviews, including
the final correctness fixes, with no blockers. Luna accepted the measurement
harness review, and Sol independently accepted the campaign evidence and
calculations with no blockers. The amd64 campaign is complete, but Phase 2
remains open and bridge choice is unset. The PR targets
`t3/opcode-grouping-oracle` as a separate stack layer.

## Scope and ownership

This is a build-tagged experiment. The Rust workspace is under `rust/`; every
Go file under `internal/opcodespike/` and the executor harness has the
`opcode_bridge_spike` build tag. The Cgo lane additionally requires
`opcode_bridge_ffi` and Cgo. No production Go runtime file, release packaging,
traffic path, or ownership boundary changed.

Go continues to own the complete `[]*state.GeneratorOpcode`, request-version
negotiation, metadata, coalescing policy and hashes, IDs, validation limits,
errors, persistence, SDK calls, and effects. Rust receives only versioned
opcode tags in original order and returns original indices and two flags. Go
materializes all slices from those indices; it never reconstructs by ID or
`op/id`, and duplicate positions remain distinct.

The kernel represents only priority grouping, the non-lazy subsequence,
history-group state, and plan capability. `hasPlanOp` is not permission to
coalesce. Empty helper input remains empty; it is distinct from HTTP parsing
that normalizes `[]` to a `None` opcode.

## Wire and FFI contract

The process and FFI paths use the same binary PLAN and RESULT frames. Every
integer is little-endian. The fixed 24-byte header is:

| Offset | Field | Width |
| --- | --- | ---: |
| 0 | ASCII `IOPC` magic | 4 bytes |
| 4 | Protocol version, currently 1 | u16 |
| 6 | Kind | u16 |
| 8 | Request ID | u64 |
| 16 | Payload length | u32 |
| 20 | Status | u32 |

Kinds are HELLO=1, READY=2, PLAN=3, RESULT=4, ERROR=5, SHUTDOWN=6, and
BYE=7. HELLO/READY use the exact 16-byte schema/capability payload: schema 1,
tag set 1, index width 4, supported result flags `0b11`, maximum 65,536 tags,
and maximum 1 MiB complete frame. READY must match it exactly. PLAN is a u32
tag count followed by one byte per tag. RESULT is three u32 counts, one u32
flags word, then priority, other, and non-lazy u32 index arrays. RESULT echoes
the request ID. ERROR has an empty payload and one fixed numeric status; it
does not contain error strings.

Go tags are explicitly mapped from the 17 recognized Go enum values in their
original order to wire values 0 through 16. The mapping uses a switch and never
casts Go enum integers. The request and result maximum complete-frame sizes
are 65,564 and 524,328 bytes. All lengths and counts are bounded before
allocation. The decoders reject trailing/truncated bytes, invalid status,
unknown kinds/versions/tags, reserved flags, request-ID mismatch, and malformed
handshakes.

The shared Go result validator checks count arithmetic, bounds, complete unique
group coverage, stable increasing order within groups, correct priority
membership, exact non-lazy order, and history/empty/plan flag consistency. The
executor harness then compares the whole projection with `opGroups`,
`NonLazyIDs`, and `hasPlanOp`. Any protocol error, timeout, cancellation, or
oracle mismatch yields no usable Rust result.

The FFI ABI is
`uint32_t opcode_eval_v1(const uint8_t*, uint32_t, uint8_t*, uint32_t, uint32_t*)`.
It accepts a complete PLAN frame and writes a complete RESULT frame into
caller-owned buffers. Rust validates nulls, bounds, address overflow, and
pairwise non-overlap before reading the request. The caller must provide
readable request memory and writable output memory; output need not be
initialized. After evaluation and capacity checks, Rust copies only the
initialized result bytes to output. A valid aligned `written` pointer is set to
zero before argument validation and remains zero on errors. Rust retains no
pointer after the synchronous call. Release panic strategy is unwind;
the entrypoint catches panics and safely contains panic-payload destruction
without installing a global panic hook. Bounded `try_reserve` calls cover the
large Rust vectors, but process-wide allocator exhaustion can still abort.

The Go FFI wrapper checks `context.Context` before and after the synchronous
C call and discards a late result. It does not claim hard cancellation during
native code and does not add a per-call goroutine. A disposable child-process
test bounds the FFI probe; no intentional native hang or abort was injected.

## Process lifecycle

The Rust worker is a single blocking stdin/stdout loop with no child threads.
Go starts only an absolute executable path with `exec.Command`, no shell, and
places it in a process group. Each worker accepts one request at a time; pool
admission is bounded to pool size plus the configured queue. The pool defaults
to one worker and supports up to eight workers for later measurements.

Startup waits at most two seconds for READY. Each request gets the earlier of
the caller deadline and one second. Shutdown waits 250 ms for BYE; process
cleanup, reaping, I/O joining, and stderr-drainer joining are bounded to one
second. One goroutine owns `cmd.Wait`. The stderr drainer retains at most 64
KiB while draining additional bytes. Queued cancellation sends no PLAN. An
in-flight timeout/cancellation, partial response, EOF, invalid frame, or
semantic validation error poisons and kills the worker; the failed request is
never retried. A pool slot restarts after cleanup at most three times with
100/200/400 ms bounded backoff. Closing the pool cancels restart timers and
active contexts.

## Test authoring gate

The test-audit authoring questions are answered as follows:

1. **Independent contract:** consume the original checked-in 27-case,
   all-17-tag Go fixture and pin literal original indices, history, plan
   capability, and payload identity across Rust and both transports.
2. **Credible regression:** a port can classify one tag incorrectly, reorder
   a group, lose a duplicate or empty ID position, mark lazy operations
   non-lazy, corrupt flags, or return structurally valid but semantically
   different indices.
3. **Existing proof:** the Go oracle proves only Go behavior. The original
   fixture did not prove a Rust implementation or its framing/lifecycle. The
   process fault helper exercises transport failures only; it does not mock
   opcode semantics. Deterministic seeded Go cases add differential inputs
   beyond the literal fixture.
4. **Owner boundary/no seam:** the Rust test consumes the original JSON with
   `serde`/`serde_json` dev dependencies; Go tests invoke the real helpers.
   No production-only test export, runtime wrapper, or injectable production
   seam was added.

## Dependency and packaging evidence

The workspace and all Rust commands use Rust 1.94.0. This toolchain is
installed with x86_64/aarch64 GNU and musl targets. `cargo +1.94.0 info`
resolved current crates.io releases `serde 1.0.229` (declared Rust 1.56) and
`serde_json 1.0.154` (declared Rust 1.71); the lockfile records those exact
versions and their transitive test dependencies. The Rust team published
[Rust 1.94.0](https://blog.rust-lang.org/2026/03/05/Rust-1.94.0/) on 2026-03-05;
crate metadata is on [serde](https://crates.io/crates/serde/1.0.229) and
[serde_json](https://crates.io/crates/serde_json/1.0.154). `cargo tree -e
normal` for `opcode-bridge` lists only `opcode-kernel`; Serde is absent from
runtime dependencies.

Build observations on the Linux/amd64 host:

- The amd64 GNU release worker is a dynamically linked x86-64 ELF. The amd64
  GNU static library linked successfully into the Cgo test executable.
- `rustc --print native-static-libs` reports, in order,
  `-lgcc_s -lutil -lrt -lpthread -lm -ldl -lc` for both x86_64 GNU and
  aarch64 GNU static libraries. Those exact flags are in the amd64/arm64 Cgo
  build-tagged linker directives.
- The aarch64 GNU static library cross-builds as an archive. A final arm64 Go
  Cgo link was not available because `aarch64-linux-gnu-gcc` is absent.
- The aarch64 musl release worker cross-builds using Rust 1.94.0's `rust-lld`
  with `CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER` and
  `RUSTFLAGS='-C link-self-contained=yes -C panic=unwind'`. `file` and
  `readelf` identify it as a statically linked AArch64 ELF executable.
- No native arm64 execution was performed. `qemu-aarch64` and
  `aarch64-linux-gnu-gcc` are absent. Docker buildx is installed, but its
  configured builder cannot connect to the Docker daemon in this environment,
  so it did not provide an arm64 runtime runner. Cross-compilation is not an
  arm64 runtime test.

The arm64 musl worker was built and inspected with:

```sh
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=/home/ashutosh/.rustup/toolchains/1.94.0-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/bin/rust-lld RUSTFLAGS='-C link-self-contained=yes -C panic=unwind' cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --bin opcode-worker --release --target aarch64-unknown-linux-musl --locked
file rust/target/aarch64-unknown-linux-musl/release/opcode-worker
readelf -h rust/target/aarch64-unknown-linux-musl/release/opcode-worker
```

The static link flags were read from each GNU target with:

```sh
cargo +1.94.0 rustc --manifest-path rust/Cargo.toml --package opcode-bridge --lib --release --target x86_64-unknown-linux-gnu --locked -- --print native-static-libs
cargo +1.94.0 rustc --manifest-path rust/Cargo.toml --package opcode-bridge --lib --release --target aarch64-unknown-linux-gnu --locked -- --print native-static-libs
```

## Validation run

Rust 1.94.0:

```sh
cargo +1.94.0 fmt --manifest-path rust/Cargo.toml --all
cargo +1.94.0 clippy --manifest-path rust/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo +1.94.0 test --manifest-path rust/Cargo.toml --workspace --locked
cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --bin opcode-worker --release --target x86_64-unknown-linux-gnu --locked
cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --lib --release --target x86_64-unknown-linux-gnu --locked
cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --lib --release --target aarch64-unknown-linux-gnu --locked
```

The Rust unit tests passed: 12 bridge tests and 4 kernel tests, including the
original fixture, frame limits, FFI pointer bounds/overlap, panic containment,
and empty/lazy grouping. The AArch64 musl command and binary inspection are
described above; it was not executed.

Go 1.26.4, Linux/amd64, using
`/home/ashutosh/.cache/go1.26.4/go/bin/go`, with
`GOTOOLCHAIN=local`,
`GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike`, and
`OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker`:

```sh
GOTOOLCHAIN=local GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike CGO_ENABLED=0 OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker /home/ashutosh/.cache/go1.26.4/go/bin/go test -tags opcode_bridge_spike ./internal/opcodespike ./pkg/execution/executor \
  -run '^(TestProcess|TestPool|TestStartup|TestStderr|TestOpcodeBridgeSpike)' -count=1

GOTOOLCHAIN=local GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike CGO_ENABLED=1 OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker /home/ashutosh/.cache/go1.26.4/go/bin/go test -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor \
  -run '^(TestProcess|TestPool|TestStartup|TestStderr|TestFFI|TestOpcodeBridge)' -count=1

GOTOOLCHAIN=local GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike CGO_ENABLED=1 OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker /home/ashutosh/.cache/go1.26.4/go/bin/go test -race -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor \
  -run '^(TestProcess|TestPool|TestStartup|TestStderr|TestFFI|TestOpcodeBridge)' -count=1

GOTOOLCHAIN=local GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike CGO_ENABLED=1 OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker /home/ashutosh/.cache/go1.26.4/go/bin/go test -tags opcode_bridge_spike,opcode_bridge_ffi ./pkg/execution/executor \
  -run '^$' -bench '^BenchmarkOpcodeBridgeSpikeSmoke$' -benchtime=1x -count=1
```

All four passed. The test runs cover all 27 fixture cases through process and
FFI, 256 seeded differential requests, repeated pointer positions, distinct
duplicate-ID payloads, eight concurrent workers/calls, bad/oversized/truncated
frames, invalid indices and flags, startup failure, cancellation, queued
cancellation, process death, bounded restart, pool close, and stderr cap. The
benchmark was a one-iteration functionality smoke only; its `ns/op` output was
not treated as performance evidence. `CGO_ENABLED=0` verifies the process path
does not require Cgo.

The task-plan status script was also run. It continues to fail only on the
pre-existing unrelated `docs/plans/run-ai-debugging.md` status
`Implemented — pending staging review`; that file is unchanged.

### FFI output initialization review follow-up (2026-10-26)

Sol review identified that `from_raw_parts_mut(output, output_capacity)`
required every output byte to be initialized, while the C ABI promises only
writable caller memory. The bridge now copies only the initialized encoded
result with `ptr::copy_nonoverlapping`, after the existing bounds, overflow,
overlap, evaluation, and capacity checks. It no longer creates a Rust slice
over the output capacity. A `MaybeUninit<u8>` regression calls the exported ABI
with uninitialized output storage and reads only the bytes reported as written.
The Cgo declaration comment now documents that output need not be initialized.

The FFI tests also cover null and misaligned `written` pointers, all three
pairwise range overlaps, and unchanged output on insufficient capacity. The
exported ABI was not directly induced to panic; without changing its production
API there is no safe panic injection point. Its private panic-containment helper
(the path used by the exported entrypoint) is tested with an injected panic and
a panic-payload destructor that also panics. No public API was added.

After the review fix, Rust 1.94.0 formatting, workspace tests (12 bridge and 4
kernel tests), Clippy with `-D warnings`, and release amd64 GNU builds of both
`opcode-worker` and the static library passed. Focused Go 1.26.4 FFI parity and
race checks passed with the rebuilt library and the cache `GOTMPDIR`:

```sh
GOTOOLCHAIN=local GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike CGO_ENABLED=1 OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker /home/ashutosh/.cache/go1.26.4/go/bin/go test -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor -run '^(TestFFIEntryDisposableProbe|TestFFIContextRejectsBeforeEnteringRust|TestOpcodeBridgeFFIFixtureDifferential|TestOpcodeBridgeFFIEightConcurrentCalls)$' -count=1

GOTOOLCHAIN=local GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike CGO_ENABLED=1 OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker /home/ashutosh/.cache/go1.26.4/go/bin/go test -race -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor -run '^(TestFFIEntryDisposableProbe|TestFFIContextRejectsBeforeEnteringRust|TestOpcodeBridgeFFIFixtureDifferential|TestOpcodeBridgeFFIEightConcurrentCalls)$' -count=1
```

Miri was not run: `cargo +1.94.0 miri --version` reports that the Miri
component is unavailable for the installed toolchain. The ordinary
`MaybeUninit` regression passed; no Miri result is claimed. The output fix does
not change runtime Go behavior, select the bridge, accept the lifecycle, or
change broad Phase 1 from partial.

### Final correctness acceptance and artifact refresh (2026-10-26)

The parent accepted the implementation reviews with no remaining correctness
blockers. The accepted fixes are: FFI writes only initialized result bytes;
process cleanup, restart, and budgets are bounded and covered; the shutdown
BYE race is fixed; and the measurement timer calls the real Go helpers, excludes
the oracle, isolates each measurement row, records RSS initial/peak/final, and
uses an FFI-only request-ID counter. No protocol, production lifecycle, or
runtime-selection change was made.

The final correctness checks were rerun on this worktree:

```sh
cargo +1.94.0 fmt --manifest-path rust/Cargo.toml --all -- --check
cargo +1.94.0 clippy --manifest-path rust/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo +1.94.0 test --manifest-path rust/Cargo.toml --workspace --locked

GOTOOLCHAIN=local GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike TMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike CGO_ENABLED=0 OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker /home/ashutosh/.cache/go1.26.4/go/bin/go test -tags opcode_bridge_spike ./internal/opcodespike ./pkg/execution/executor -run '^(TestProcess|TestPool|TestStartup|TestStderr|TestOpcodeBridgeSpike)' -count=1

GOTOOLCHAIN=local GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike TMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike CGO_ENABLED=1 OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker /home/ashutosh/.cache/go1.26.4/go/bin/go test -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor -run '^(TestProcess|TestPool|TestStartup|TestStderr|TestFFI|TestOpcodeBridge)' -count=1

GOMAXPROCS=1 GOTOOLCHAIN=local GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike TMPDIR=/home/ashutosh/.cache/go-tmp-inngest-opcode-spike CGO_ENABLED=1 OPCODE_WORKER_BIN=/home/ashutosh/.t3/worktrees/inngest/t3-755ee0e7/rust/target/x86_64-unknown-linux-gnu/release/opcode-worker /home/ashutosh/.cache/go1.26.4/go/bin/go test -race -tags opcode_bridge_spike,opcode_bridge_ffi ./internal/opcodespike ./pkg/execution/executor -run '^(TestProcess|TestPool|TestStartup|TestStderr|TestFFI|TestOpcodeBridge)' -count=3
```

All passed. Rust ran 12 bridge and 4 kernel tests. The Go process-only run
passed with Cgo disabled; the combined process/FFI run passed with Cgo enabled;
the repeated race run passed at `GOMAXPROCS=1`.

Release artifacts were refreshed or confirmed current with Rust 1.94.0:

```sh
cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --bin opcode-worker --release --target x86_64-unknown-linux-gnu --locked
cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --lib --release --target x86_64-unknown-linux-gnu --locked
cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --lib --release --target aarch64-unknown-linux-gnu --locked
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=/home/ashutosh/.rustup/toolchains/1.94.0-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/bin/rust-lld RUSTFLAGS='-C link-self-contained=yes -C panic=unwind' cargo +1.94.0 build --manifest-path rust/Cargo.toml --package opcode-bridge --bin opcode-worker --release --target aarch64-unknown-linux-musl --locked
```

`file`/`readelf` identify the x86-64 GNU worker as a dynamic ELF PIE and the
AArch64 musl worker as a static AArch64 ELF. The AArch64 GNU static library is
an archive. The AArch64 GNU Go Cgo final link remains unavailable because
`aarch64-linux-gnu-gcc` is absent; `qemu-aarch64` is also absent. No native
arm64 execution or performance result is claimed. Artifact SHA-256 values and
the environment snapshot are in the [measurement campaign report](opcode-bridge-spike-measurements.md)
and the [completed results artifact](opcode-bridge-spike-results.json).

## Measurement results and remaining gates

The accepted campaign ran on Linux/amd64: three seeded runs with 96 rows each,
10,000 measured calls per row, six 500-sample process startup groups per run,
and 18 serial benchmark commands with ten one-second repetitions each. All 21
commands exited 0. Per-route matched ratios, per-run p50/p95/p99, closed-loop
throughput, aggregate host/worker RSS, resource snapshots, benchmark means and
allocations, exact command statuses, and raw-file hashes are in the [human
results report](opcode-bridge-spike-results.md) and [structured results
artifact](opcode-bridge-spike-results.json).

The measured output shows substantial decoded/process overhead under the
proposed uncalibrated thresholds. The thresholds remain unchanged and failures
are preserved as outcomes. The accepted startup P2 is disclosed: startup
groups are fixed-order observations before the seeded steady-state shuffle,
not randomized startup comparisons, and no cold-cache comparison was
controlled. FFI initialization is not treated as process startup. The durable
artifacts retain summaries and hashes, not the individual 10,000 request
durations per row, so the reported percentiles cannot be independently
reconstructed from the committed files. Go `B/op` excludes Rust allocator
activity. The host was unpinned and loaded, and three-run variance limits the
selection value of median ratios; only 2/16 full-response FFI cells pass all
three metrics on all three runs, despite the median-based 8/16 result.

Next gates:

1. On native Linux/arm64, execute the process worker and complete the final GNU
   FFI Cgo link/runtime check. Until then, no arm64 performance result is
   claimed.
2. Keep broad Phase 1 partial and Phase 2 in progress with bridge choice
   unset. FFI hard cancellation is unavailable, and `CGO_ENABLED=0` process
   packaging versus FFI policy remains unresolved. Full parity, rollout,
   recovery, platform, cloud, and retirement gates remain unchanged.
3. If the spike continues, the next bounded experiment is a direct FFI caller
   buffer path that avoids the wire-frame round trip. It is an experiment only
   and makes no committed bridge choice.
