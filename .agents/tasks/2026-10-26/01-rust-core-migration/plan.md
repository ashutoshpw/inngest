# Convert the Go Execution Core to Rust

**Date:** 2026-10-26

**Task:** 01-rust-core-migration

## Objective

Migrate the Inngest execution core from Go to Rust in behavior-preserving stages, retaining persisted-state and SDK replay compatibility. Completion means the covered runtime has no Go execution or queue algorithm owner.

## Decisions and scope

- Target the OSS/self-host runtime evidenced here. First bridge validation targets Linux amd64/arm64. macOS and Windows validation is deferred, not retired, and is required before broader release or Go retirement. Cloud parity is a separate gate.
- Preserve currently implemented SDK request, request-version, and replay behavior. The configured SDK pins do not confirm minimum supported SDK versions; see the [SDK and deployment matrix](references/sdk-deployment-matrix.md).
- Eventual Rust scope includes trigger/CEL and schedule decisions, run/step transitions, queue coordination, state adapters, and recovery. UI, SDK implementations, and management API rewrites remain out of scope; gateways may stay behind explicit ports.
- No Rust workspace or bridge is selected. Compare FFI and a local process at the bridge spike; keep decision logic deterministic and I/O-free.
- **Sequencing amendment adopted 2026-10-26:** broad Phase 1 stays partial. The bounded Linux amd64/arm64 read-only bridge **entry gate is satisfied** by the accepted exact-slice Go oracle, recorded Go microbenchmarks, and bounded existing DB/Valkey/lifecycle evidence. Phase 2 is not done and no bridge is selected. Each later Rust migration needs its relevant additional-owner baseline first. Full rollout/retirement gates remain unchanged; see the [bounded bridge contract](references/opcode-bridge-contract.md).

## Delivery

Use dedicated worktrees and small stacked PRs. Verify and record the first layer's base before implementation; each later layer targets its preceding branch. Follow the repository's conventional-title and PR-text rules in [validation and rollout](references/validation-rollout.md).

## Work packages

- [ ] **1. Freeze baseline — partial.**
  - [x] SDK request wire and replay-shaped input corpus implemented and independently Luna-reviewed with no blockers; malformed-case and reset-overhead-label findings are fixed. See the [corpus README](../../../../pkg/execution/driver/testdata/sdk-request-v1/README.md).
  - [x] Opcode-plan and generator-parser fixtures accepted after independent Sol-high review found no blockers; three P2 findings were fixed, with Go validation and mutation evidence in the [opcode baseline](references/opcode-plan-baseline.md).
  - [x] CEL/SQL portable fixtures and owner-boundary tests accepted as stack layer 2 after independent Sol-high review; the benchmark path was renamed to `evaluator_program_construction_cached_parse`, its source comment explains lifted-literal parser-cache reuse, and ten samples were rerun. See the [CEL/SQL compatibility baseline](references/cel-sql-compatibility.md).
  - [x] Queue-item JSON and representative initial run-state fixtures accepted by the parent as partial stack layer 3 after independent Sol-high review; the sole P2 finding on loaded `PriorityFactor` and `ParallelCoalesceKey` is fixed and revalidated. The recovery witness is a separate accepted layer 4 and is not part of layer 3. See the [queue/state compatibility baseline](references/queue-state-compatibility.md).
  - [x] SDK response -> checkpoint -> fresh-runtime replay witness accepted after independent Luna-max review found no blockers; real HTTP, surviving stores, worker retry, and both key-queue modes are covered. This replaces drained owners, not an OS process or active lease. See the [recovery baseline](references/recovery-baseline.md) and [fixture README](../../../../tests/execution/executor/testdata/sdk-response-replay-v1/README.md).
  - [x] Bounded pure opcode bridge entry gate satisfied: 27 cases/all 17 tags, literal original indices and payload identity; focused/race checks and independent Luna review accepted with no blockers. See the [bridge contract](references/opcode-bridge-contract.md) and [bounded Go service evidence](references/go-service-baseline.md).
  - [ ] Complete the Go reference baseline: SDK-response replay, CEL/SQL server semantics and broader operator coverage, broader queue/state persistence and recovery, broader lifecycle/fault coverage, and initial Go performance baselines; confirm SDK/platform policy with owners. CEL/SQL and queue/state have partial Go fixtures. Rust parity and Go/Rust/bridge comparisons belong to later phases. See the [CEL/SQL compatibility baseline](references/cel-sql-compatibility.md).
- [ ] **2. Bridge spike.** After the amended bounded entry gates above pass, compare FFI and process prototypes on Linux amd64/arm64 using the index-only opcode contract; measure correctness, packaging, cancellation, memory, startup, and bridge cost. Full Rust fixture parity and matched Go/Rust/bridge performance follow as the Rust slices land.
- [ ] **3. First Rust slice.** Port only pure opcode grouping/non-lazy original indices, history flags, and plan capability. Go retains negotiation, metadata, coalescing eligibility/hash/IDs, limits/errors, writes, SDK calls, and effects. Broader step-plan intents need a separately baselined layer.
- [ ] **4. CEL/SQL and core parity.** Have Rust consume the CEL/SQL fixtures and compare evaluation plus SQLite/Postgres filter behavior against the Go baseline; then migrate the remaining trigger, schedule, and run/step decisions.
- [ ] **5. Persistence and recovery.** Port queue/state algorithms only after persisted formats, atomicity, ownership, and recovery pass parity; Go remains the sole Goose migration owner.
- [ ] **6. Fenced rollout.** Expand shadow and production ownership by run/partition only after parity and recovery gates pass.
- [ ] **7. Retire Go core.** Retire Go algorithms after parity, deferred-platform validation, release, recovery, and rollback gates pass; retain a validated Go rollback artifact.

## Acceptance criteria

- [ ] Full wire/replay, CEL/SQL, queue/state, fault, recovery, and lifecycle corpus has no unexplained differences; effect ordering and idempotency behavior match.
- [ ] Before rollout, matched Go/Rust/bridge performance is measured and gates calibrated; no migration performance threshold is inferred from the current Go microbenchmarks.
- [ ] Supported-target builds, compatible persisted state, single ownership, staged rollout, and rollback are validated before Go retirement.

## References

- [Runtime map and proposed boundary](references/runtime-map.md) — ownership, bridge scope, and decision boundary.
- [Behavior and persistence contracts](references/behavior-contracts.md) — observable replay, CEL, lifecycle, queue, and storage behavior.
- [SDK and deployment matrix](references/sdk-deployment-matrix.md) — configured CI/build coverage, first Linux bridge targets, and deferred gates.
- [Opcode-plan baseline](references/opcode-plan-baseline.md) — fixture scope, review status, test evidence, mutations, and benchmarks.
- [Bounded opcode bridge contract](references/opcode-bridge-contract.md) — sequencing amendment, index representation, audit rationale, validation, and accepted bounded entry gate.
- [Go service baseline](references/go-service-baseline.md) — accepted bounded DB/Valkey/lifecycle evidence at the PR #4 snapshot, commands, skips, and limits.
- [CEL/SQL compatibility baseline](references/cel-sql-compatibility.md) — portable CEL evaluation and SQLite/Postgres filter fixtures, owner-boundary tests, and Go benchmark observations.
- [Queue/state compatibility baseline](references/queue-state-compatibility.md) — persisted queue-item and representative initial run-state values, owner-boundary tests, and Go microbenchmarks.
- [Recovery baseline](references/recovery-baseline.md) — accepted surviving-store replay witness, validation, and limits.
- [Phase 1 baseline discovery](references/baseline-discovery.md) — source-grounded coverage and remaining baseline work.
- [Validation and rollout](references/validation-rollout.md) — stack order, parity commands, rollout, release, and rollback gates.
