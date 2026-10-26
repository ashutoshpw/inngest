# Opcode plan and generator parser baseline

**Phase:** 1, partial increment

**Review:** Accepted partial increment. Sol's first independent review found three P2 findings, which were fixed. A fresh Sol-high review found no blockers and confirmed ordering/failure behavior, negative cases at the actual production gate, isolated coverage for all seven plan-capable opcode families, passing focused/race checks, and no runtime-source changes.

**Source Go commit:** `3624c37d5285805d2a95efd0715e8913d5f81ae8`

**Branch/base:** `rust-core/01-compatibility-baseline` at the source commit above

The later [bounded bridge prerequisite](opcode-bridge-contract.md) extends the
accepted 20-case corpus to 27 cases with additive literal original-index
expectations. That increment is accepted after independent Luna review found no blockers; it does not
change this source provenance or retroactively extend the accepted review and
validation evidence below. Coalescing eligibility remains Go-owned.

## Scope

This increment freezes two deterministic boundaries for later cross-language
consumption:

- `pkg/execution/executor/testdata/compatibility/opcode-plan-v1.json` records
  opcode inputs and literal grouping, non-lazy ID, history-group, and
  `hasPlanOp` answers. Its response-handler vectors record pending-step calls,
  coalescing-key propagation, metadata updates, enqueue counts, and handler
  failures at the actual `HandleGeneratorResponse` owner boundary.
- `pkg/execution/driver/httpdriver/testdata/compatibility/generator-parser-v1.json`
  records legacy single-object and array parsing, empty-array normalization,
  output absent/null distinctions, malformed JSON, null entries, unknown
  enums, and StepError retry-header handling. Its Go test calls
  `ParseGenerator` directly from the parser owner package.

Both fixtures carry schema versions and source file/commit provenance. Expected
answers are checked-in literals; tests never regenerate them from the code
under test. No production code, exported API, or test-only production seam was
added.

The opcode vectors pin priority-then-other grouping and stable filtering order
within each group. The executor handles members of one group concurrently, so
the fixtures intentionally make no claim about sibling effect order. They also
pin that DeferAdd/DeferAbort remain priority operations but do not enter
NonLazyIDs, including when IDs are duplicated or empty. WaitForSignal is
plan-capable and nonpriority in the source implementation.

Isolated helper vectors pin each `hasPlanOp` opcode family: StepPlanned,
WaitForEvent, WaitForSignal, Sleep, InvokeFunction, Gateway, and AIGateway.
The mixed signal/wait vector remains a separate grouping case. Expected
coalescing outcomes are checked only through response-handler fixtures; the
pure-helper test does not rebuild the production gate as its own oracle.

The response-handler cases cover stored versus response request-version
selection, successful SavePending before each recorded opcode-handler effect
(state saves, queue enqueues, and scheduled callbacks), and a SavePending failure
that returns to the caller before enqueue or handler effects.
Negative gate cases include ordinary parallel steps without a plan-capable op,
multiple completion opcodes, and one planned nonlazy opcode with a lazy defer.
The event recorder is mutex-protected because opcode handlers run concurrently;
the tests do not order sibling effects. The completion fixture intentionally
omits SyncRunComplete data and stops at its decode error, so it covers the gate
without claiming finalization coverage. The lazy DeferAdd takes its existing
soft-reject path and is only a pending-count witness. Existing owner-boundary
integration tests remain responsible for defer finalization, persisted
pending-step lifecycle, invoke discovery fan-in, and enqueue retry behavior.

## Test-audit record

1. **Contract protected:** deterministic opcode grouping and plan-op
   classification, lazy-step accounting, coalescing eligibility and key
   propagation, SavePending-before-fanout ordering and failure short-circuit,
   exact cross-language hash and discovery job ID, plus parser wire-shape/error
   behavior.
2. **Credible regressions:** priority classification or stable filtering could
   change; lazy IDs could inflate pending counts; one of the plan-capable
   opcodes could be dropped; completed-only or one-nonlazy-plus-lazy responses
   could acquire a coalesce key; SavePending could move after fan-out or its
   failure could permit handler effects; stored-version precedence or
   ForceStepPlan could change; hash framing could drift between Go and Rust; or
   parser absent/null/error handling could diverge.
3. **Why current coverage did not subsume this:** existing Go tests cover many
   of these properties and lifecycle effects, but did not provide a shared
   versioned corpus with literal exact hash/job-ID answers or portable parser
   vectors. The earlier handler fixtures checked only planned-step pairs and
   reconstructed the gate in the test, so they did not independently guard its
   negative conditions or write ordering. The new tests do not replay lifecycle
   assertions owned by `tests/execution/executor`.
4. **Production seam:** none. Tests run in the owning packages and call current
   helpers or production owners directly.

## Hash known answers

The fixture pins these `xxhash64` outputs in lowercase hex:

| Input | Key |
| --- | --- |
| Run `01ARZ3NDEKTSV4RRFFQ69G5FAV`, IDs `step-a`, `step-b` | `1af438d9048ad7aa` |
| Same run, reversed IDs | `1af438d9048ad7aa` |
| Same run, `step-a`, `step-a`, `step-b` | `13fa418a9b022a06` |
| Run `01ARZ3NDEKTSV4RRFFQ69G5FAW`, IDs `step-a`, `step-b` | `28e50f3e49623c23` |
| Same run, IDs `ab`, `c` | `3c31dddddcf3d938` |
| Same run, IDs `a`, `bc` | `53fda8ff0609031c` |

The production discovery owner produces the pinned ID
`01ARZ3NDEKTSV4RRFFQ69G5FAV-1af438d9048ad7aa-discover` for the first vector.
The delimiter pair preserves the distinction between those two normal SDK ID
sets; the fixture does not claim safety for IDs containing embedded NUL bytes.

## Validation

The repository requires Go 1.26.4. No suitable local toolchain was present, so
the official `go1.26.4.linux-amd64.tar.gz` was installed at
`/home/ashutosh/.cache/go1.26.4/go` without replacing `/usr/bin/go`. The SHA-256
from the official Go download metadata was verified before extraction:

```text
1153d3d50e0ac764b447adfe05c2bcf08e889d42a02e0fe0259bd47f6733ad7f
```

The following passed with `GOTOOLCHAIN=local` and that explicit toolchain path:

```text
go test ./pkg/execution/executor -run '^(TestOpcodePlanCompatibilityFixture|TestOpcodePlanCompatibilityAtResponseHandler|TestOpcodePlanCoalesceHashKnownAnswers|TestOpcodePlanDiscoveryJobIDKnownAnswer)$' -count=1
  ok github.com/inngest/inngest/pkg/execution/executor

go test -race ./pkg/execution/executor -run '^(TestOpcodePlanCompatibilityFixture|TestOpcodePlanCompatibilityAtResponseHandler|TestOpcodePlanCoalesceHashKnownAnswers|TestOpcodePlanDiscoveryJobIDKnownAnswer)$' -count=1
  ok github.com/inngest/inngest/pkg/execution/executor

go test ./pkg/execution/executor ./pkg/execution/driver/httpdriver -count=1
  ok github.com/inngest/inngest/pkg/execution/executor
  ok github.com/inngest/inngest/pkg/execution/driver/httpdriver

go test ./tests/execution/executor -run '^(TestDeferAdd|TestDeferAbort|TestParallelPauseBackedOpsCoalesceDiscovery|TestFinalizeRetriesFnFinishedAfterPublishFailure|TestFinalizeDeleteFailureDoesNotLoseFinishEffects)$' -count=1
  ok github.com/inngest/inngest/tests/execution/executor
```

The executor package run includes the existing `TestDiscoveryEnqueueFailureAlwaysRetries`
and the current discovery/hash tests. Both JSON fixture files parsed as valid
JSON, and all changed Go test/benchmark files were formatted with Go 1.26.4
`gofmt`.

Temporary production mutation probes were run one at a time and restored
`executor.go` to its original SHA-256
(`f9a53b9fee1eda4534b6e3814b0f9f7c0126e4b402c732db16a4ec7d09338e88`)
after each probe. Moving SavePending after fan-out failed the ordering assertion
and the failed-save no-effects assertion. Removing `hasPlanOp` from the gate
failed the ordinary-step and completion-only pending-call assertions. Removing
the nonlazy-count guard failed the one-planned-step-plus-lazy-defer assertion.
These probes changed no final production source.

## Benchmark baseline

Command:

```sh
GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test ./pkg/execution/executor \
  -run '^$' -bench '^BenchmarkOpcodePlan$' -benchmem -benchtime=100ms -count=10
```

Environment: Go 1.26.4, Linux/amd64, AMD Ryzen 9 5950X 16-Core Processor. Each
row below summarizes ten 100 ms samples; time is the min / median / max
`ns/op`. Allocation counts were stable across repetitions except where a
small byte range is shown.

| Input | Pre-decoded pure grouping path | JSON decode plus same planning path |
| --- | --- | --- |
| Small, 1 opcode | 92.59 / 112.85 / 146.9 ns/op; 24 B/op; 2 allocs/op | 5,007 / 5,939.5 / 7,619 ns/op; 928 B/op; 18 allocs/op |
| Mixed, 8 opcodes | 635.3 / 885.05 / 1,149 ns/op; 488 B/op; 12 allocs/op | 23,044 / 27,723 / 34,169 ns/op; 3,872 B/op; 73 allocs/op |
| Large, 1,000 opcodes | 103,360 / 133,266 / 170,547 ns/op; 66,120–66,126 B/op; 32 allocs/op | 2,054,072 / 3,238,189 / 3,733,470 ns/op; 422,504–422,508 B/op; 6,052 allocs/op |

The pre-decoded path covers grouping, NonLazyIDs, plan-op detection, and the
coalesce hash when eligible. The decode path adds JSON unmarshalling into a
`state.DriverResponse`; it is not an HTTP/network or queue lifecycle benchmark.
These are repeatable Go microbenchmark observations, not p95 latency or a
calibrated migration gate. The large decode range is noisy at 100 ms and should
be rerun with a longer matched workload before setting thresholds.

## Gaps and next baseline work

This opcode/parser increment is not completion of work package 1. The separate
[SDK request V1 corpus](../../../../../pkg/execution/driver/testdata/sdk-request-v1/README.md)
now covers outgoing `MarshalV1` wire bytes and replay-shaped request inputs, but
it does not pair those requests with SDK responses to validate full replay.
Remaining work includes CEL evaluation and SQLite/Postgres query parity,
queue/state persistence, broader lifecycle/fault injection, owner-confirmed SDK
version policy, and the cross-boundary performance suite. It does not exercise
Sleep, InvokeFunction, Gateway, or AIGateway handlers through
their external collaborators; their plan-capability classification is pinned
at the production pure-helper boundary. Existing integration tests cover invoke
coalescing and defer lifecycle. The new handler fixture does not claim sibling
effect order.

The parser fixture is bounded to `ParseGenerator`. It does not freeze
`ParseResponse`, `ParseRetry`, stream-response decoding, oversized opcode
validation, HTTP status/header handling, or the full legacy/current SDK matrix.
