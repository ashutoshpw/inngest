# Convert the Go Execution Core to Rust

**Date:** 2026-10-26

**Task:** 01-rust-core-migration

## Objective

Migrate the Inngest execution core from Go to Rust in behavior-preserving stages, retaining persisted-state and SDK replay compatibility. Completion means the covered runtime has no Go execution or queue algorithm owner.

## Decisions and scope

- Target the OSS/self-host runtime evidenced here. First bridge validation targets Linux amd64/arm64. macOS and Windows validation is deferred, not retired, and is required before broader release or Go retirement. Cloud parity is a separate gate.
- Preserve currently implemented SDK request, request-version, and replay behavior. The configured SDK pins do not confirm minimum supported SDK versions; see the [SDK and deployment matrix](references/sdk-deployment-matrix.md).
- Eventual Rust scope includes trigger/CEL and schedule decisions, run/step transitions, queue coordination, state adapters, and recovery. UI, SDK implementations, and management API rewrites remain out of scope; gateways may stay behind explicit ports.
- The bridge spike compares FFI and a local process; bridge choice remains unset until independent review, matched measurements, and platform gates are complete. Keep decision logic deterministic and I/O-free.
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
  - [x] Accept the bounded staged-batch Go owner corpus and bounded component-cost evidence. Literal output/recipe/barrier/version/clock witnesses and forty Go-only component samples passed focused/race checks; independent Luna round two found no blockers after the sibling-concurrency P2 repair. Keep the cost STOP: full staged projection/validation/materialization costs and absolute sleep clocks remain unmeasured. A future Rust-owned continuation with Go effect ports needs a separately implemented and reviewed Go response-owner extraction; this layer does not implement or approve that extraction. See the [staged batch reference](references/staged-batch-reference.md).
  - [ ] Complete the Go reference baseline: SDK-response replay, CEL/SQL server semantics and broader operator coverage, broader queue/state persistence and recovery, broader lifecycle/fault coverage, and initial Go performance baselines; confirm SDK/platform policy with owners. CEL/SQL and queue/state have partial Go fixtures. Rust parity and Go/Rust/bridge comparisons belong to later phases. See the [CEL/SQL compatibility baseline](references/cel-sql-compatibility.md).
- [ ] **2. Bridge spike — implementation and amd64 evidence accepted; Phase 2 remains open; bridge choice unset.** The isolated, build-tagged Rust/process/FFI prototype and correctness/build evidence are recorded in the [opcode bridge spike implementation](references/opcode-bridge-spike.md). Independent Sol review accepted the measurement evidence: all 21 commands, 288 latency rows across three seeds, 180 serial benchmark repetitions, source/raw hashes, and calculations match with no blockers. The uncalibrated proposed thresholds still show substantial decoded/process overhead and were not relaxed. The report also records why the median-based full-response FFI result is not a robust selection signal. Native arm64 execution and the final GNU Cgo link/runtime check remain open; broad Phase 1 is partial, Phase 2 is not complete, and no bridge is selected. See the [human results report](references/opcode-bridge-spike-results.md) and [structured results](references/opcode-bridge-spike-results.json).
- [x] **2a. Direct-buffer FFI — safety, fairness, and representative amd64 screening accepted; stop full-matrix expansion.** Independent Luna safety and evidence reviews found no blockers; rust-direct-ffi-screening-review-01 verified the 54 paired ratios, 18 summaries, 63 cached artifact hashes, source/binary manifests, and serial summaries. Sol accepted the unique-job-index fairness fix and ran the representative screen: all 15 screening and seven setup/build commands passed, with 72 latency rows and 60 serial repetitions. Only 2/6 direct cells pass the unchanged, uncalibrated criteria. Keep Phase 2 open, broad Phase 1 partial, bridge unset, and production wiring absent; this result does not establish broad Rust infeasibility. A larger-boundary read-only investigation remains separate and pending. See the [implementation](references/opcode-bridge-direct-ffi.md) and [screening evidence](references/opcode-bridge-direct-ffi-screening.md).
- [ ] **3. First Rust slice — response-continuation hypothesis, gated.** After the two tiny bridge screens and staged-owner investigation, prepare a Rust-owned whole-batch continuation with Go effect ports. Future candidate: None/Step/StepRun/StepError/StepFailed/StepPlanned only; any excluded or invalid descriptor keeps the whole batch in Go. No routing or Rust implementation is approved by this Go extraction.
  - [x] First bounded Go response-owner extraction accepted after Sol effects review and independent Luna-max staging round two. The real response entry uses a private runner for staging/group joins and accepted-save continuations, concrete Go effects, and checkpoint reuse. Baseline/current/race and extraction integration witnesses are recorded in the [response-owner extraction](references/response-owner-extraction.md). This does not activate or select Rust; retryable/planned sequencing, finer queue ports, host lifetime and recovery remain open.
- [ ] **4. CEL/SQL and core parity.** Have Rust consume the CEL/SQL fixtures and compare evaluation plus SQLite/Postgres filter behavior against the Go baseline; then migrate the remaining trigger, schedule, and run/step decisions.
- [ ] **5. Persistence and recovery.** Port queue/state algorithms only after persisted formats, atomicity, ownership, and recovery pass parity; Go remains the sole Goose migration owner.
- [ ] **6. Fenced rollout.** Expand shadow and production ownership by run/partition only after parity and recovery gates pass.
- [ ] **7. Retire Go core.** Retire Go algorithms after parity, deferred-platform validation, release, recovery, and rollback gates pass; retain a validated Go rollback artifact.

## Acceptance criteria

- [ ] Full wire/replay, CEL/SQL, queue/state, fault, recovery, and lifecycle corpus has no unexplained differences; effect ordering and idempotency behavior match.
- [ ] Before rollout, matched Go/Rust/bridge performance is measured and gates calibrated; no migration performance threshold is inferred from the current Go microbenchmarks.
- [ ] Supported-target builds, compatible persisted state, single ownership, staged rollout, and rollback are validated before Go retirement.

## References

- [Response-owner extraction](references/response-owner-extraction.md) — bounded Go staging/join and save-continuation work, invariants, verification, and remaining Rust integration gates.
- [Direct FFI representative screening](references/opcode-bridge-direct-ffi-screening.md) — accepted independent safety, fairness, and Luna evidence reviews; 2/6 direct cells pass and full-matrix expansion stops.
- [Direct FFI structured screening](references/opcode-bridge-direct-ffi-screening.json) — exact command/status/hash and resource audit, all 54 paired comparisons, 18 summaries, and 60 serial repetitions.
- [Direct FFI paired runs](references/opcode-bridge-direct-ffi-screening-paired.csv) and [serial repetitions](references/opcode-bridge-direct-ffi-screening-benchmarks.csv) — all 54 ratios and 60 benchmark repetitions.

- [Direct-buffer FFI experiment](references/opcode-bridge-direct-ffi.md) — ABI safety reasoning, test gate, correctness and smoke checks, retained limits, and review handoff.
- [Staged batch Go reference](references/staged-batch-reference.md) — accepted bounded owner corpus, sibling-concurrency mutation evidence, component-cost STOP, and separate extraction gaps.
- [Staged component samples](references/staged-batch-cost-benchmarks.csv) and [raw benchmark output](references/staged-batch-cost-benchmarks.txt) — forty Go-only component samples and original byte counts; full projection, validation, and materialization costs remain unmeasured.

- [Runtime map and proposed boundary](references/runtime-map.md) — ownership, bridge scope, and decision boundary.
- [Behavior and persistence contracts](references/behavior-contracts.md) — observable replay, CEL, lifecycle, queue, and storage behavior.
- [SDK and deployment matrix](references/sdk-deployment-matrix.md) — configured CI/build coverage, first Linux bridge targets, and deferred gates.
- [Opcode-plan baseline](references/opcode-plan-baseline.md) — fixture scope, review status, test evidence, mutations, and benchmarks.
- [Bounded opcode bridge contract](references/opcode-bridge-contract.md) — sequencing amendment, index representation, audit rationale, validation, and accepted bounded entry gate.
- [Opcode bridge spike implementation](references/opcode-bridge-spike.md) — isolated Rust kernel, process/FFI transports, correctness and packaging evidence, limits, and remaining gates.
- [Opcode bridge spike measurement campaign](references/opcode-bridge-spike-measurements.md) — accepted harness review, campaign scope, schema, environment, and remaining gates.
- [Opcode bridge spike measurement results](references/opcode-bridge-spike-results.md) — per-route threshold outcomes, serial benchmarks, ordered startup observations, resource caveats, and exact command statuses.
- [Opcode bridge spike structured results](references/opcode-bridge-spike-results.json) — all per-run latency/resource rows, paired ratios, benchmark repetitions, command details, and raw artifact hashes.
- [Go service baseline](references/go-service-baseline.md) — accepted bounded DB/Valkey/lifecycle evidence at the PR #4 snapshot, commands, skips, and limits.
- [CEL/SQL compatibility baseline](references/cel-sql-compatibility.md) — portable CEL evaluation and SQLite/Postgres filter fixtures, owner-boundary tests, and Go benchmark observations.
- [Queue/state compatibility baseline](references/queue-state-compatibility.md) — persisted queue-item and representative initial run-state values, owner-boundary tests, and Go microbenchmarks.
- [Recovery baseline](references/recovery-baseline.md) — accepted surviving-store replay witness, validation, and limits.
- [Phase 1 baseline discovery](references/baseline-discovery.md) — source-grounded coverage and remaining baseline work.
- [Validation and rollout](references/validation-rollout.md) — stack order, parity commands, rollout, release, and rollback gates.
