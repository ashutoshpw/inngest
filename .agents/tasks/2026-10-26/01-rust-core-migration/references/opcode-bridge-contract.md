# Bounded opcode bridge prerequisite

## Scope and sequencing amendment

The parent explicitly adopted this amendment on 2026-10-26: broad Phase 1
remains partial, while a pure, read-only Linux amd64/arm64 opcode bridge spike
may start after acceptance of the exact-slice Go oracle, recorded Go
microbenchmarks, and bounded existing DB/Valkey/lifecycle evidence. The parent
has accepted all three: the bounded pure Linux amd64/arm64 read-only bridge
**entry gate is satisfied**. Independent Luna oracle review found no blockers.
The separate [Go service baseline](go-service-baseline.md) records all 11
successful invocations (161 top-level passes, three events skipping the same
uniqueness-blocked test) at the PR #4 snapshot, with reconstructed commands and
temporary-log limits. This does not establish arm64 or Rust validation. Phase 2
is not completed or selected; this layer adds no Rust workspace, prototype,
runtime change, or production traffic.

Before each later Rust migration, establish the relevant additional-owner Go
baseline: CEL/SQL server semantics before CEL/SQL migration; persisted formats,
atomicity, leases, recovery, and lifecycle/fault behavior before their owners
move. This is an explicit narrower entry gate, not completion of broad Phase 1.
Rollout and Go retirement still require full coverage, recovery, calibrated
matched Go/Rust/bridge performance, deferred platform validation, and separate
cloud gates. Current SDK behavior is preserved; minimum supported SDK version
policy remains owner-unconfirmed and unchanged.

## Proposed representation (no bridge implemented)

Go retains the complete decoded `[]*state.GeneratorOpcode` for the duration of
the decision. Send only an ordered array of opcode tags to the pure kernel;
array positions are implicit zero-based original indices. Do not send or
normalize IDs, payloads, options, errors, timing, metadata, or userland fields.

Return these fields, with no effects or metadata proposals:

```json
{
  "priorityIndices": [1, 3],
  "otherIndices": [0, 2],
  "nonLazyIndices": [1, 3, 0, 2],
  "historyGroup": true,
  "hasPlanOp": true
}
```

These are the literal first fixture case's positions. Priority and other arrays
partition the original positions exactly once, preserving input order inside
each group. Non-lazy positions follow priority-then-other order, retaining
duplicates of IDs and empty IDs as distinct positions. Both groups carry the
same history flag. Empty input returns three empty arrays and two false flags.
The field `hasPlanOp` is plan capability only, not permission to coalesce.

Go indexes the original array to obtain complete opcodes and non-lazy IDs;
never reconstruct full opcodes from `op/id`, use IDs as an index, deduplicate
positions by ID, or change opaque IDs. The future adapter must reject malformed
results (out-of-range, missing/repeated group positions, invalid ordering or
inconsistent non-lazy positions) before considering them usable. In read-only
shadow, disagreement or bridge failure is diagnostic; Go remains authoritative
and alone applies effects. Bridge framing, integer widths, versioning, buffer
ownership, cancellation, panic containment, and failure behavior require design
and validation in the next spike; this JSON illustrates the decision shape,
not an adopted wire transport.

The domain is non-null, recognized opcode entries after Go parsing/validation.
No nil-op or unknown-enum contract is added: HTTP rejects null entries, whereas
some internal helpers skip nil and `hasPlanOp` dereferences entries. Existing Go
handling remains authoritative for inputs outside this bounded domain. An
empty helper batch is distinct from HTTP `ParseGenerator` normalizing `[]` to
one `None` opcode; both existing parser and helper contracts are retained.

## Ownership retained in Go

`HandleGeneratorResponse` retains request-version selection and metadata writes,
ForceStepPlan and HasAI updates, limits/validation, handled timestamps, history
IDs, error/retry handling, pending-step persistence and its ordering before
fan-out, coalescing eligibility, hashing and discovery IDs, queue/state/SDK I/O,
and all opcode handler effects. The bridge gets no request version or run ID
and returns no coalescing key or eligibility flag.

In particular, `Metadata.ShouldCoalesceParallelism` chooses the stored version
unless it is `-1`, then uses the response version, and compares `>= 2`. It is
not a closed enum gate. The existing real-handler vectors `(-1,2)`, `(1,2)`,
`(2,1)`, and `(2,2)` remain Go-owned evidence. Stored `0`, negotiated `0/1`,
and future versions above `2` are deliberately not new bridge vectors in this
layer. Moving that gate later requires those literal real-owner version
vectors first; copying its predicate into a helper test would not suffice.

## Fixture and authoring audit

Read-only discovery covered `AGENTS.md`, the test-audit skill and workflow,
task docs, `util.go`, `enums/opcode.go`, `hasPlanOp`, metadata version selection,
the response-handler caller, existing util/compatibility tests, and benchmark.
The parent already selected this bounded authoring step after the audit;
proceeding uses that explicit authorization rather than reopening discovery
approval. No delegation or production seam is needed.

1. **Independent contract:** the portable cross-language fixture pins literal
   original indices, stable priority/group order, non-lazy membership/order,
   history flags, and plan capability. Five missing ordinary opcode families
   (`None`, legacy `Step`, `StepError`, `StepFailed`, `DiscoveryRequest`) and
   an empty batch now have isolated cases; a mixed ordinary/lazy batch pins
   history behavior without any plan-capable op. There are 27 cases in total.
2. **Credible failures:** a port could drop `None`, treat errors/discovery as
   lazy or plan-capable, conflate identical `op/id` pairs, reorder groups,
   inflate history using lazy defers, or mishandle empty input.
3. **Why existing proof is insufficient:** the accepted 20-case fixture lacked
   those opcode cases and original-index answers. Go-only util tests are not a
   portable oracle. Extend the existing primary fixture test, without adding
   ordinary-opcode handler integration or duplicating lifecycle proof. All 17
   currently recognized opcode tags now occur, without treating that inventory
   as a substitute for broad lifecycle coverage.
4. **Real owner, no new production seam:** `TestOpcodePlanCompatibilityFixture`
   invokes `opGroups`, `NonLazyIDs`, `nonLazyOpCount`, and `hasPlanOp` directly.
   It maps returned group pointers to original indices without reimplementing
   priority/lazy predicates. Literal non-lazy indices are independently mapped
   to original IDs and checked against the literal ID answer and production
   `NonLazyIDs`. A second real-owner projection on shallow opcode copies with
   unique index labels as IDs checks the exact non-lazy positions, including
   duplicates/empty original IDs, without copying a classification predicate.
   This does not claim Go already returns non-lazy indices. Fixture input now
   decodes full production opcodes; two identical `StepRun/dup` entries carry
   distinct names/data/options and must retain their original pointers.

All original provenance remains fixed at source Go commit
`3624c37d5285805d2a95efd0715e8913d5f81ae8`. Index fields are additive literal
expectations in `opcode-plan-v1`; no production oracle regenerates them. No
runtime helpers were added solely for tests. Existing handler, hash, discovery,
and parser fixtures remain their own stronger boundaries for those contracts.

## Workspace and validation

Worktree: `/home/ashutosh/.t3/worktrees/inngest/t3-7f9a4100`.
Branch: `t3/opcode-grouping-oracle`; the initial check found `t3/7f9a4100`
clean at layer-1 head
`a4592cde49cab4c4a1afc5488cf39345514d5ed7`, then fast-forwarded with
`git merge --ff-only f2fcebce2ec7cb1528f04114d4a9fb43930ad765`.
The accepted layer-4 head is this increment's base. During implementation an
external branch rename was observed in the reflog from `t3/7f9a4100` to
`t3/opcode-grouping-oracle`; no agent checkout or branch rename was performed.
The same worktree/current branch binding is retained. The parent authorized
accepted layer-5 delivery on this branch, stacked on PR #4; no other worktree
is changed.

Validation on 2026-10-26 used Go 1.26.4, Linux/amd64, AMD Ryzen 9 5950X,
`GOTOOLCHAIN=local`, and
`GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-t3-7f9a4100` (the default `/tmp`
was 98% full). All commands below use the explicit binary
`/home/ashutosh/.cache/go1.26.4/go/bin/go`:

```sh
go test -p=2 ./pkg/execution/executor ./pkg/execution/driver/httpdriver -count=1
# PASS: executor 5.557s, httpdriver 1.259s

go test -p=2 ./pkg/execution/executor ./pkg/execution/driver/httpdriver \
  -run '^(TestOpcodePlanCompatibilityFixture|TestOpcodePlanCompatibilityAtResponseHandler|TestOpcodePlanCoalesceHashKnownAnswers|TestOpcodePlanDiscoveryJobIDKnownAnswer|TestGeneratorParserCompatibilityFixture)$' -count=1
# PASS after final position-label assertion: executor 0.037s, httpdriver 0.021s

go test -p=2 -race ./pkg/execution/executor ./pkg/execution/driver/httpdriver \
  -run '^(TestOpcodePlanCompatibilityFixture|TestOpcodePlanCompatibilityAtResponseHandler|TestOpcodePlanCoalesceHashKnownAnswers|TestOpcodePlanDiscoveryJobIDKnownAnswer|TestGeneratorParserCompatibilityFixture)$' -count=1
# PASS after final position-label assertion: executor 1.233s, httpdriver 1.141s

GOMAXPROCS=1 go test -p=2 ./pkg/execution/executor -run '^$' \
  -bench '^BenchmarkOpcodePlan$' -benchmem -benchtime=100ms -count=10
# PASS, six workloads with ten samples each
```

The full owner-package run preceded the final position-label assertion; the
final focused and race runs include it. No broad full-repository, arm64, DB,
Valkey, or lifecycle suite was run by this layer. Formatting and
`git diff --check` pass. `scripts/check-plan-status.sh` still exits 1 for the
pre-existing unrelated status `Implemented — pending staging review` in
`docs/plans/run-ai-debugging.md`; that file is unchanged.

A fixture-only negative control swapped the two duplicate-ID non-lazy positions
from `[0,1,2]` to `[0,2,1]`. The complete `TestOpcodePlanCompatibilityFixture`
run failed at the intended position-label assertion (`["0","2","1"]` versus
`["0","1","2"]`), even though the original ID projection is identical. The
fixture was restored byte-for-byte (SHA-256
`08baf8227440703d0c98533995f828db837dc9eb4219ef6dfa844866471d08b0`), and the
complete fixture test passed again. An initial subtest-filtered attempt also
hit the existing full-corpus name-count assertion; the complete-parent retry
removed that unrelated failure. No production source was mutated.

The existing benchmark is unchanged. Its `pure_grouping` path includes
`opGroups`, `NonLazyIDs`, `hasPlanOp`, and the Go coalescing hash when eligible;
`decode_end_to_end` additionally decodes a `DriverResponse`. Neither measures
the proposed index adapter or tag-only Rust kernel, HTTP, IPC, queue lifecycle,
p95, or peak memory. The following min / median / max ns/op observations were
recorded with other validation running on the same host; they are not isolated
matched measurements or migration thresholds:

- Small (1 opcode): pure 100.8 / 106.6 / 135.6, 24 B/op, 2 allocs/op;
  decode 4,612 / 4,900.5 / 5,384, 928 B/op, 18 allocs/op.
- Mixed (8 opcodes): pure 851.4 / 875.05 / 896.4, 488 B/op, 12 allocs/op;
  decode 20,778 / 22,247 / 26,343, 3,872 B/op, 73 allocs/op.
- Large (1,000 opcodes): pure 126,488 / 129,299.5 / 132,979, 66,120 B/op,
  32 allocs/op; decode 2,196,304 / 2,286,879 / 2,663,746, 422,504 B/op,
  6,052 allocs/op. Allocation values were constant over all ten samples.

These recorded Go observations meet the bounded prerequisite's microbenchmark
recording requirement only; the parent has accepted oracle review and the
separately recorded bounded service evidence.
Measure the actual narrowed Go/Rust/index-bridge paths on matching workloads
and both Linux architectures in the spike before selecting a bridge or claiming
performance parity.

Independent Luna review is accepted with no blockers for the 27-case, all-17-tag
oracle, literal index expectations, and payload identity. The separately accepted
[service evidence](go-service-baseline.md) closes only the bounded entry subgate;
broad Phase 1 and all later rollout/retirement gates remain open.
