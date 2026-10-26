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

## Delivery

Use dedicated worktrees and small stacked PRs. Verify and record the first layer's base before implementation; each later layer targets its preceding branch. Follow the repository's conventional-title and PR-text rules in [validation and rollout](references/validation-rollout.md).

## Work packages

- [ ] **1. Freeze baseline — partial.**
  - [x] SDK request wire and replay-shaped input corpus implemented and independently Luna-reviewed with no blockers; malformed-case and reset-overhead-label findings are fixed. See the [corpus README](../../../../pkg/execution/driver/testdata/sdk-request-v1/README.md).
  - [x] Opcode-plan and generator-parser fixtures accepted after independent Sol-high review found no blockers; three P2 findings were fixed, with Go validation and mutation evidence in the [opcode baseline](references/opcode-plan-baseline.md).
  - [ ] Complete SDK-response replay, CEL/SQL, queue/state persistence, recovery and broader lifecycle/fault coverage, full performance baselines, and owner-confirmed SDK/platform policy.
- [ ] **2. Bridge spike.** Compare FFI and process prototypes on Linux amd64/arm64; measure correctness, packaging, cancellation, memory, startup, and end-to-end cost.
- [ ] **3. First Rust slice.** Port pure SDK-opcode classification/grouping and step-plan intents; Go remains the sole owner of writes, SDK calls, and effects.
- [ ] **4. CEL/SQL and core parity.** Stack CEL evaluation plus SQLite/Postgres filter parity next, then migrate the remaining trigger, schedule, and run/step decisions.
- [ ] **5. Persistence and recovery.** Port queue/state algorithms only after persisted formats, atomicity, ownership, and recovery pass parity; Go remains the sole Goose migration owner.
- [ ] **6. Fenced rollout.** Expand shadow and production ownership by run/partition only after parity and recovery gates pass.
- [ ] **7. Retire Go core.** Retire Go algorithms after parity, deferred-platform validation, release, recovery, and rollback gates pass; retain a validated Go rollback artifact.

## Acceptance criteria

- [ ] Full wire/replay, CEL/SQL, queue/state, fault, recovery, and lifecycle corpus has no unexplained differences; effect ordering and idempotency behavior match.
- [ ] Matched Go/Rust/bridge performance is measured and gates calibrated; no migration performance threshold is inferred from the current Go microbenchmarks.
- [ ] Supported-target builds, compatible persisted state, single ownership, staged rollout, and rollback are validated before Go retirement.

## References

- [Runtime map and proposed boundary](references/runtime-map.md) — ownership, bridge scope, and decision boundary.
- [Behavior and persistence contracts](references/behavior-contracts.md) — observable replay, CEL, lifecycle, queue, and storage behavior.
- [SDK and deployment matrix](references/sdk-deployment-matrix.md) — configured CI/build coverage, first Linux bridge targets, and deferred gates.
- [Opcode-plan baseline](references/opcode-plan-baseline.md) — fixture scope, review status, test evidence, mutations, and benchmarks.
- [Phase 1 baseline discovery](references/baseline-discovery.md) — source-grounded coverage and remaining baseline work.
- [Validation and rollout](references/validation-rollout.md) — stack order, parity commands, rollout, release, and rollback gates.
