# Phase 1 baseline discovery

**Branch:** `rust-core/01-compatibility-baseline`

**Target/base:** `main`, verified at `3624c37d5285805d2a95efd0715e8913d5f81ae8` on `origin/main` and `upstream/main`

**Worktree start:** `3624c37d5285805d2a95efd0715e8913d5f81ae8`

The initial inventory was read-only outside this task folder. Subsequent
baseline increments and their evidence are recorded in the [opcode baseline](opcode-plan-baseline.md)
and [SDK request corpus README](../../../../../pkg/execution/driver/testdata/sdk-request-v1/README.md).
The current completion and review state is in [task status](../status.md).

## Existing evidence and gaps

### Opcode grouping and step planning

- `pkg/execution/executor/util.go:20-140` owns the current pure helpers:
  `opGroups`, non-lazy counting/IDs, group order, and the parallel coalesce key.
  `pkg/execution/executor/executor.go:3958-4080` is the consuming boundary: it
  persists the SDK request/hash version, enables step planning when there are
  multiple non-lazy operations, shares one handling timestamp, saves pending
  IDs before fan-out, and waits for the priority group before the other group.
- `pkg/execution/executor/util_test.go:14-410` already covers priority grouping,
  defer add/abort priority, single versus multiple non-lazy ops, lazy ID
  exclusion, and coalesce-key determinism/order independence/collision
  separation. The coalesce-key test checks properties but does not pin a literal
  key value. Several checks assert helper structs directly, so they are not a
  shared Go/Rust corpus.
- Stronger behavior witnesses already exist in
  `tests/execution/executor/defer_test.go` (`TestDeferAdd` and
  `TestDeferAbort`) and
  `tests/execution/executor/parallel_invoke_coalesce_test.go`.
  `TestDeferAdd` includes defer piggybacked with `RunComplete`, a bare defer,
  and a parallel plan whose pending set excludes lazy IDs. The coalescing test
  checks the resulting discovery enqueue count. These are owner-boundary tests;
  do not add a second copy of those assertions.
- `tests/execution/executor/queuedat_test.go` has in-code opcode scenarios
  replayed through the executor, but they are Go structs, not fixtures usable
  by a Rust consumer. Generated group history IDs and timestamps must either
  remain Go effects or be supplied as fixed inputs before they can be included
  in deterministic parity output.

### SDK encoding and replay

- `pkg/execution/driver/driver.go:105-269` contains `MarshalV1`. It loads
  events, defer metadata, and step state; normalizes a missing event `user` to
  an empty object; builds the request with request/hash version and dispatch
  IDs; filters action state using the stack; includes memoized inputs; switches
  to API reads when state/request size is large; and canonicalizes final JSON
  with JCS. `pkg/execution/driver/request.go:44-122` defines the wire names and
  optional request IDs.
- The [SDK request V1 corpus](../../../../../pkg/execution/driver/testdata/sdk-request-v1/README.md)
  now pins complete `MarshalV1` bytes, including replay-shaped stack inputs,
  request/hash versions, malformed inputs, and size fallbacks. It covers request
  serialization only; it is not a paired SDK-response replay harness. The
  separate opcode baseline records `ParseGenerator` response parsing coverage.
- `pkg/execution/state/README.md` says changing request data can break replay;
  it describes request/hash version `-1` as unknown, legacy version `0`, and
  version `1` as the documented `stepID:index` hash. The request corpus now pins
  the current wire behavior across these request-version inputs, but does not
  confirm minimum supported SDK versions or replay against a full response
  corpus. Confirm the supported SDK/version matrix with its owner rather than
  inferring it from configured test pins.

### CEL and event matching

- `pkg/expressions/expressions_test.go`, `unknown_test.go`,
  `validation_test.go`, `lifted_evaluation_test.go`, and `residual_test.go`
  already cover a broad evaluator surface: missing versus explicit null,
  nested attributes, numeric coercion, truthy `||`/`&&`, ternaries, unknown and
  partial evaluation, macros, `uppercase`/`lowercase`, `b64decode`, and
  `json_parse`. There are evaluator benchmarks in `expressions_test.go` and
  `lifted_evaluation_test.go`.
- `pkg/run/cel.go` owns event matching and expression-to-SQL conversion.
  `pkg/run/cel_test.go` covers matching against event/output data and checks
  generic, SQLite, and Postgres filter expressions, including boolean/null
  conversion and unsupported predicates. These are Go-native tables, not a
  shared JSON oracle. A Rust CEL implementation must preserve both evaluation
  and database filter behavior.

### Queue, state, and lifecycle

- `pkg/execution/queue` has broad algorithm tests for item conversion,
  constraints, backlog, leases, retries, and iterator behavior.
  `pkg/execution/queue/convert_test.go` checks queue item/proto round trips.
  `pkg/execution/state/redis_state` has detailed Redis-state tests for queue
  persistence, idempotency, priority, leases, requeue, backlog, and cleanup;
  most use miniredis and assert Go types or Redis indexes directly. Lua script
  source snapshots live under
  `pkg/execution/state/redis_state/testdata/snapshots/`.
- `tests/execution/executor/helpers_test.go` wires the actual executor to
  SQLite plus in-memory Redis. Its integration tests cover finalization,
  publish/delete failures and retries
  (`tests/execution/executor/finalize_duplicate_test.go`), defer lifecycle,
  sleep retry, and parallel discovery coalescing. They are strong Go owner
  witnesses but do not produce a versioned cross-language output file.
- The Valkey compatibility tests in `tests/execution/queue/lua_compatibility_test.go`
  and `tests/execution/state_store/lua_compatibility_test.go` use
  testcontainers; they validate storage-server behavior separately from the
  miniredis unit suite. DB schema parity checks are in `pkg/db` and include the
  documented PostgreSQL command in `references/validation-rollout.md`.
- The opcode baseline records a deterministic opcode-plan benchmark, and the
  SDK request corpus README records `MarshalV1` and JCS microbenchmarks. Existing
  expression and Redis-state benchmarks cover other components. These Go
  microbenchmarks do not supply an end-to-end executor or bridge p95 baseline;
  `BenchmarkPeekTiming` uses miniredis unless `REDIS_ADDR` is set.

## Current baseline increments

The opcode-plan and generator-parser fixtures, owner-boundary Go checks, and
Go microbenchmark are accepted after a fresh Sol-high review found no blockers;
the three P2 findings from the first review were fixed. The SDK request
wire/replay-shaped input corpus is accepted after independent Luna review found
no blockers and the malformed-case and reset-overhead-label findings were fixed.
A partial CEL/SQL Go oracle now records versioned evaluator cases and prepared
SQLite/Postgres filter fragments; its authoring rationale and validation are in
the [CEL/SQL compatibility baseline](cel-sql-compatibility.md). These are Go
reference baselines, not evidence of Rust parity. See the linked evidence above
for fixture scope, review, and validation results.

Partial queue/state persistence coverage is accepted as stack layer 3 after
independent Sol-high review found one P2 in loaded `PriorityFactor` and
`ParallelCoalesceKey` assertions; those literal pointer expectations were added
and revalidated with focused and race tests plus a targeted mutation probe. It
pins three queue-item JSON cases through the Redis enqueue/store/load owner path
and a bounded initial run-state New/store/Load sample. The fixture contract,
audit rationale, validation, and microbenchmark are recorded in the
[queue/state compatibility baseline](queue-state-compatibility.md). A separate
process-restart/recovery witness is under independent review in worktree
`t3/recovery-replay-witness`; it is not included in this layer. This layer does
not cover real Redis/Valkey behavior or Rust parity.

## Remaining phase 1 Go baseline

- **SDK response/replay:** pair the now-pinned `MarshalV1` requests with SDK
  generator responses and verify replayed step/hash state, including agreed
  legacy/current SDK behaviors. The existing request corpus already covers
  canonical bytes, replay-shaped stack inputs, request/hash versions, defer
  abortability, and size fallbacks; supported minimum SDK versions remain
  unconfirmed.
- **CEL:** the new [CEL/SQL compatibility baseline](cel-sql-compatibility.md)
  covers a small portable vector set for missing/null/nested values, numeric and
  truthy coercion, a custom function, partial/unknown evaluation, errors, event
  matching, and SQLite/Postgres filter fragments and arguments. Broader Go-side
  CEL/SQL coverage and database-server semantics remain open.
  Validate the rendered SQL behavior against the supported SQLite/PostgreSQL
  versions as part of the Go reference baseline. Rust fixture consumption and
  differential parity are Phase 4 work after the bridge spike and first Rust
  slice; they are not Phase 1 entry gates.
- **Queue/state:** the partial [queue/state compatibility baseline](queue-state-compatibility.md)
  pins representative queue-item JSON and initial run-state values. Stable
  persisted IDs beyond these cases, Redis key/TTL and real-server Lua behavior,
  process restart/recovery, idempotent duplicate enqueue, lease expiry,
  priority wait/defer, backlog and reservation contention, semaphore release,
  and SQLite/Postgres schema compatibility remain open.
- **Lifecycle/faults:** capture ordered observable effects for schedule, step
  discovery/fan-out, retry, pause/resume, cancellation, defer add/abort,
  completion/finalization, duplicate deliveries, shutdown/restart, and failures
  on each boundary between state writes, enqueue, output save, claim, child
  scheduling, and finalization. Preserve deduplication/at-least-once behavior;
  do not specify exactly-once execution.
- **Performance:** benchmark matching Go workloads for opcode planning, SDK
  encoding, CEL, and queue/state operations, with fixed inputs and recorded Go
  version, CPU/OS, database/server, flags, and fixture size. These are initial
  Go baselines for Phase 1. `go test -bench` provides ns/op, throughput, and
  allocations; matched Go/Rust/bridge comparisons and a matched-load harness for
  p95, peak memory, and end-to-end bridge cost follow in Phases 2–4. Keep
  proposed thresholds provisional until those measurements exist.

## Suggested commands and local prerequisites

These are proposed commands only; none was run during discovery. Use the Go
version required by `go.mod` first.

```sh
go test ./pkg/execution/executor -run 'TestOpGroups|TestNonLazyOpCount|TestOpcodeGroups_IDs_ExcludesLazyOps|TestComputeParallelCoalesceKey' -count=1
go test ./pkg/execution/executor -run '^$' -bench '^BenchmarkOpcodePlan$' -benchmem -count=10
go test ./pkg/execution/driver/... ./pkg/execution/state/... -count=1
go test ./pkg/expressions ./pkg/run -count=1
go test ./tests/execution/executor -run 'TestDefer(Add|Abort)|TestParallelPauseBackedOpsCoalesceDiscovery|TestFinalize(PublishesFnFinishedOnce|RetriesFnFinishedAfterPublishFailure|DeleteFailureDoesNotLoseFinishEffects)' -count=1
go test ./pkg/expressions -run '^$' -bench '^(BenchmarkEvaluate|BenchmarkEvaluateParallel|BenchmarkLiftedExpressionEvaluate)$' -benchmem -count=10
```

For the comprehensive phase, also use the focused Valkey/SQLite/Postgres and
full-suite checkpoints in `references/validation-rollout.md`. Testcontainers
compatibility tests need Docker and a pullable Valkey image. PostgreSQL parity
needs a configured PostgreSQL test database (CI starts PostgreSQL 17). E2E
validation needs pnpm dependencies plus the dev server/SDK fixtures on ports
3000 and 8288. Most state/executor unit tests instead use miniredis and the
in-memory SQLite adapter.

Observed local toolchain/build state on this worktree:

- `/usr/bin/go` is Go `1.18.1`; `go.mod` requires `1.26.4`. Go `1.26.4` was
  installed at `/home/ashutosh/.cache/go1.26.4/go` and used for the recorded
  focused baseline validation. The repo's `.envrc` selects its Nix flake, and
  `flake.nix` supplies Go and build tools; CI uses `go-version-file` from
  `go.mod`. `.tool-versions` records Node `22.16.0` and pnpm `10.18.2`.
- Rust `1.99.1` and Cargo `1.99.1` are installed, but only the host target
  `x86_64-unknown-linux-gnu` is installed. This checkout has no `Cargo.toml`,
  `Cargo.lock`, `.rs` files, or pinned Rust toolchain/crate selection.
- GCC 11, make, pkg-config, protoc 34.1, SQLite CLI, Docker, Node 22, and pnpm
  are present. Clang, CMake, native `redis-server`/`redis-cli`, `psql`, and `buf`
  are absent. Docker is available for the repository's testcontainer-based
  Valkey/PostgreSQL suites. No native Rust dependency requirements can be
  concluded before selecting crates or a bridge.

## Open inputs and blockers

- The system Go binary remains too old; use the installed Go 1.26.4 path or the
  repository Nix environment for further baseline validation.
- The repo documents hash-version behavior but does not define the supported
  SDK release matrix or minimum versions for this migration; confirm the agreed
  SDK versions and full replay guarantees with the SDK owners.
- This checkout only establishes OSS/self-host ownership. Cloud services and a
  cloud parity owner remain unconfirmed for any broader rollout.
- There is no Rust workspace or chosen bridge yet, so this phase can define the
  fixture contract and Go reference baseline, but cannot produce Rust parity
  results.
