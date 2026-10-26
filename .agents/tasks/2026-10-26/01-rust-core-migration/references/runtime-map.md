# Runtime map and proposed boundary

## Current ownership

- [`pkg/devserver/devserver.go`](../../../../../pkg/devserver/devserver.go) wires the Redis state v2 adapter, queue processor, executor, and legacy runner. The [runner README](../../../../../pkg/execution/runner/README.md) describes `runner` as a legacy self-host entrypoint; it is not the future Rust boundary.
- [`pkg/execution/execution.go`](../../../../../pkg/execution/execution.go) exposes a broad `execution.Executor` interface. Its `Schedule` comment says rate limiting, debouncing, and expression filtering happen elsewhere, but [`executor.Schedule`](../../../../../pkg/execution/executor/executor.go) performs constraints, rate limits, debounce, state creation, and enqueue. `Execute` calls the SDK driver and handles its response; [`executor/service.go`](../../../../../pkg/execution/executor/service.go) dispatches queue work and retries.
- `state/v2.RunService` still wraps the legacy v1 manager through [`redis_state/v2_adapter.go`](../../../../../pkg/execution/state/redis_state/v2_adapter.go). [`queue.Queue`](../../../../../pkg/execution/queue) is a useful port boundary, but neither adapter is itself the complete execution core.
- The current source tree has no `Cargo.toml`, Cargo lockfile, or `.rs` files. No specific Rust crate or version is endorsed by this plan.

## Eventual Rust-owned scope

Rust ownership must grow beyond the first pure kernel to cover the current core's behavior:

1. Trigger matching, expression evaluation, and schedule decisions, including filtering, rate limits, debounce, and idempotency decisions.
2. Run and step transitions: execute/discover SDK responses, generator opcodes, retries, pause/resume, signals, invoke completion, cancellation, and finalization.
3. Queue coordination algorithms and state-adapter logic, including atomic claims, dedupe, concurrency, priority, deferred work, and recovery semantics.

Redis and SQL remain external storage engines, accessed through Rust-owned state adapters; Rust owns queue coordination and the covered queue/state algorithms. Go API and Connect gateways and SDK protocol transport clients may remain behind explicit ports. The covered runtime's end state has no Go execution or queue algorithm owner. UI, SDK implementations, and management API rewrites are not part of this plan. This target reflects OSS/self-host evidence; a cloud owner must confirm any additional services and contracts.

## First bounded slice

Start with pure SDK-opcode grouping, non-lazy original indices, history flags,
and plan capability. The [bounded bridge contract](opcode-bridge-contract.md)
defines the proposed index representation and the explicitly amended entry
gates while broad Phase 1 remains partial. Go retains complete original opcode
objects and opaque IDs; the kernel only needs ordered opcode tags. Go indexes
the originals rather than reconstructing opcodes from `op/id`. Request-version
negotiation, metadata writes, coalescing eligibility/hash/discovery IDs,
limits/errors, timestamps/history IDs, pending-step writes, state/queue
mutations, SDK calls, and effects remain Go-owned. Broader step-plan intents
need a separately baselined layer. Shadow Rust decisions read-only against Go;
do not let shadow evaluation apply writes or effects. The existing grouping
seam and tests are in [`executor/util.go`](../../../../../pkg/execution/executor/util.go)
and [`executor/util_test.go`](../../../../../pkg/execution/executor/util_test.go).

When schedule decisions migrate later, preserve mutable preconditions as atomic check-and-mutate or reservation operations. Read observations may inform a proposal but never authorize a reservation. If an atomic precondition fails, re-evaluate with the same stable IDs and the defined retry path; do not apply stale observations as a decision.

This is a proposed API boundary, not an API already present in this repository. Keep each transition small enough to compare Go and Rust decisions without allowing both to apply effects.

## Bridge and proposed layout

The first bridge comparison and validation targets Linux amd64 and arm64, as
recorded in the [SDK and deployment matrix](sdk-deployment-matrix.md). macOS
and Windows remain configured release targets whose bridge validation is
deferred until later parity gates; complete it before broader release or Go retirement.
Cloud parity is a separate gate and is not established by this OSS runtime map.

Prototype both bridge options, starting with the agreed Linux targets:

- **FFI:** compare Linux packaging first; later extend validation to Windows and macOS amd64/arm64. Define panic containment, cancellation, buffer ownership, and memory behavior.
- **Local process:** compare startup, IPC serialization, cancellation/termination, memory, and latency; define child lifecycle and health handling.

Measure end-to-end decision latency and throughput under representative load, not just kernel calls. Choose one interface only after the same correctness and deployment matrix is exercised. If either misses the proposed gates, revise the boundary or stop rather than selecting an unmeasured crate.

Proposed workspace shape (names and boundaries subject to spike):

```text
rust/
  Cargo.toml                    # workspace; proposed
  crates/core-model/            # IDs, wire/persisted models; proposed
  crates/core-decision/         # deterministic transitions and ordered effects; proposed
  crates/core-bridge/            # selected Go/Rust boundary; proposed
```

Do not commit to specific third-party crates or versions until the spike confirms serialization, CEL, FFI/process, platform, and licensing needs.

## Open decisions and risks

- Cloud deployment/services and the owner who can validate cloud parity are not established by this OSS checkout; treat cloud parity as a separate gate.
- Supported SDK versions and request/hash compatibility requirements need an explicit matrix. Configured test pins do not confirm minimum supported SDK versions or the full legacy response/replay corpus.
- Team size, sequencing capacity, and calendar estimates are unmeasured; add estimates only after the bridge and first slice are sized.
- Initial non-goals are UI, SDK, management API, and gateway rewrites. Revisit only through a separate scope decision.
