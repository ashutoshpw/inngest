# Validation and rollout

## Mandatory stacked-PR implementation workflow

- Use a dedicated Git worktree for each migration branch. Deliver the migration as small, logically scoped stacked PRs targeting GitHub repository `ashutoshpw/inngest`.
- Before implementation starts, verify the intended target repository and choose, verify, and record the first layer's base branch. Do not assume the base is `main`. The first PR targets that verified base; each dependent PR targets the immediately preceding branch in the stack. Preserve this order through review and merge.
- Keep each PR independently reviewable: limit it to a coherent change, include its applicable validation evidence, and carry forward only the dependencies needed by later layers.
- Use conventional commit-style PR titles with a type accepted by the repository's commit and changelog configuration (see `AGENTS.md`). Preserve the existing PR sections: `Description` explains the change and review context; `Release note` states user-visible impact or `None`; `Migration note` gives upgrade, deployment, config, schema, compatibility, rollout, or rollback guidance, or `None`. Follow [plan 004](../../../../../docs/plans/004-automated-release-cycle.org); do not introduce a second release-note convention.

## Baseline and parity work

**Sequencing amendment adopted 2026-10-26:** broad Phase 1 stays partial. The
bounded Linux amd64/arm64 pure read-only opcode spike may begin after acceptance
of its exact-slice Go oracle, recorded Go microbenchmarks, and bounded existing
DB/Valkey/lifecycle evidence. The parent accepted independent Luna oracle
review with no blockers and the separate [Go service evidence](go-service-baseline.md);
the bounded entry gate is satisfied. Phase 2 is not completed or selected,
and no Rust workspace exists. See the
[bounded bridge contract](opcode-bridge-contract.md). Each later Rust migration
requires the relevant additional-owner Go baseline first. The full corpus,
recovery, matched-performance, deferred-platform, cloud, rollout, and retirement
gates below are unchanged. Minimum SDK version policy remains unconfirmed.

Phase 1 remains partial. The SDK request wire and replay-shaped input corpus is accepted after independent Luna review found no blockers; malformed-case and reset-overhead-label findings are fixed. The opcode-plan and generator-parser corpora are accepted after a fresh Sol-high review found no blockers and the first review's three P2 findings were fixed. Focused Go/race tests and mutation evidence are recorded. This does not complete SDK-response replay or the broader baseline. See [task status](../status.md), the [SDK request corpus](../../../../../pkg/execution/driver/testdata/sdk-request-v1/README.md), and the [opcode baseline](opcode-plan-baseline.md).

Complete the remaining corpus with Go outputs and ordered effects for representative schedule, event match, SDK responses/replay, retries, pauses/resumes, cancellation, generator discovery, parallel fan-out, defers, sleeps/waits, and finalization. Include priority wait plus lazy DeferAdd/DeferAbort combinations, including defer before completion; verify lazy ops do not inflate pending-step counts. Compare Rust output byte-for-byte where the wire format is contractual and structurally where ordering or dynamic JSON is contractual. Add injected failures before and after state writes, queue enqueue, output persistence, claims, child scheduling, and finalization. Include duplicate deliveries and shutdown/restart recovery.

After the bridge spike and pure opcode slice, stack implementation layers in this order: **CEL evaluation plus SQLite/Postgres filter parity**, then **persistence and recovery** (state adapters, queue behavior, atomic claims, leases, and restart/failure recovery). Keep Go as the sole writer and migration owner until each relevant parity gate passes; see the [runtime map](runtime-map.md).

Benchmark the same workloads with the Go baseline, Rust kernel, and selected bridge. The plan proposes initial gates of zero unexplained fixture mismatches, p95 decision latency at most 1.10× Go, throughput at least 0.90× Go, and peak memory at most 1.20× Go. Measure during the spike, then calibrate or explicitly revise those gates before activation; existing Go microbenchmarks in the linked baselines do not establish matched bridge performance or these thresholds.

## Platform and service gates

Start FFI/process bridge validation on Linux amd64 and arm64. Defer macOS and Windows bridge validation to later parity gates; do not remove those configured release targets, and complete their validation before broader release or Go retirement. Cloud parity is a separate gate for cloud deployment; Linux bridge results in this OSS checkout do not close it. The configured targets and CI evidence are summarized in the [SDK and deployment matrix](sdk-deployment-matrix.md).

## Planned validation commands

None of these suites were run for this planning task. Use them as later checkpoints, alongside repository CI:

```sh
go test ./pkg/execution/driver/... ./pkg/execution/state/... ./pkg/connect/... ./pkg/execution/realtime/... -count=1
go test ./tests/execution/executor -run '^TestDefer' -count=1
go test ./pkg/db/... -count=1
TEST_DATABASE=postgres go test ./pkg/db -run 'TestSchemaColumnsMatchSqlc|TestCrossDialectSchemaParity' -count=1
make test
make lint
make e2e
```

The Postgres command requires the configured test database and setup dependencies. `make e2e` runs `tests.sh`; CI also exercises Go/JS SDKs, dev server/API ports 3000/8288, pnpm, SQLite/Postgres, and keyqueue settings. Run focused suites during iteration, then full test/lint/e2e and the repository's UI/codegen/build checks before release. Do not claim a command passed until it has actually run.

`pkg/db/regression_test.go` and the DB package parity tests are part of the schema baseline. During mixed-language operation, Go remains the sole Goose migration owner. Rust may read/write existing compatible schemas through a single owner, but do not introduce two migration owners, dual writes, or duplicate queue consumers.

## Rollout and rollback gates

Read-only differential shadow begins with the opcode slice and continues across core parity (phases 3–4), before production activation. Shadow uses captured/replayed or mirrored inputs and must never enqueue, persist, call an SDK, or emit a live side effect. Phase 6 is the broader stateful-adapter rollout, with fenced ownership by run or queue partition; validation does not depend on activating Rust production traffic first.

Proposed operational metrics and staged expansion gates (all thresholds are proposals to calibrate against matched Go baselines; no measurements are claimed here):

| Stage | Proposed evidence before expanding |
| --- | --- |
| Read-only shadow | Zero unexplained differences across the full witness corpus and at least 10,000 decisions on each high-volume path; use controlled replay to cover lower-volume cases. Decision p95 ≤1.10× Go. |
| 1% canary / one fenced partition | Hold at least 24 hours and cover peak-load and recovery witnesses. Queue-lag p95 stays within the existing SLO and ≤1.10× matched Go; retries and execution/state/queue error rates rise by no more than 0.5 percentage points; no unexplained increase in stuck runs or externally visible duplicate effects. |
| 10%, then 50% | Hold each stage at least 24 hours with the same gates, plus throughput ≥0.90× Go and successful shutdown/restart, failed-finalization, and lease-recovery witnesses within existing recovery SLOs. |
| 100% / retirement | All covered workload families pass the gates, ownership fencing holds, and rollback from the pinned Go artifact is exercised against Rust-written compatible state. |

Witness workloads must include schedule boundaries and reservation contention; trigger/CEL filters including missing, null, and partial data; SDK request/hash versions and replay; opcode priority, lazy defers, wait/sleep, and parallel coalescing; retries, pause/resume, cancellation, and finalization; duplicate delivery; saturated queue/state errors; shutdown/restart and failed-finalization recovery. Track queue lag and p95 latency, stuck-run count and age, retry rate, duplicate-effect attempts and externally visible duplicates, execution/state/queue error rates, throughput, memory, and recovery duration. Do not wait indefinitely for long sleeps or scheduled runs to finish: transfer fenced ownership with persisted state, stable IDs, and recovery; drain active leases before switching owners.

Permit Go fallback only before Rust has applied effects. After effects start, retain that owner until work is drained or resume from Go-readable persisted state using existing idempotent IDs/keys. If leases are active, drain or recover them before rollback. Stop rollout if state is unreadable by the rollback Go version, schema compatibility is unproven, ownership fencing fails, or a behavior/operational gate fails.

After Go source retirement, retain an immutable, pinned, validated Go artifact for each supported target, together with its source commit and build metadata. Keep it through the agreed rollback/recovery window and verify it can read Rust-written compatible state before relying on it as the rollback path.

This is at-least-once execution with idempotent/deduplicated effects, not an exactly-once guarantee. Name a rollout owner and calibrate the proposed thresholds against measured workload baselines during implementation.

## Build, release, and neighboring plans

- Preserve Go packaging requirements while Rust is linked or bundled. [`Dockerfile.goreleaser`](../../../../../Dockerfile.goreleaser), [GoReleaser](../../../../../.goreleaser.yml), and the [release workflow](../../../../../.github/workflows/release.yml) configure Linux, Windows, and macOS targets, including amd64 and arm64. Validate Linux amd64/arm64 first; validate deferred Windows/macOS targets before broader release or Go retirement.
- Retain UI build and code generation jobs that remain in the product build. Release PR text and prerelease behavior must follow [plan 004](../../../../../docs/plans/004-automated-release-cycle.org), including the `Description`, `Release note`, and `Migration note` contract.
- Current plan-status snapshot: 001 Done 48/48; 002 Draft 13/38; 003 Active 58/85; 004 Active 37/40; 005 Active 76/163. Plan 002 conformance is incomplete, plan 003 Connect work is in progress, and plan 005 CQRS changes are in progress; confirm owners/dependencies before sequencing storage and API work.
- `scripts/check-plan-status.sh` also reports the unrelated `docs/plans/run-ai-debugging.md` status `Implemented — pending staging review` as unsupported. Do not change that plan as part of this task.

## Risks and decisions to carry

- Validate cloud parity as a separate gate and confirm supported SDK versions with owners; configured test pins do not establish minimum SDK versions.
- Keep scope to execution core. UI, SDK, management API, and gateway rewrites need separate plans and compatibility gates.
- Do not estimate team size or calendar duration before the interface spike and first slice are sized.
- Do not select Rust crates or versions by assumption; document measured performance, platform, license, and behavior findings first.
