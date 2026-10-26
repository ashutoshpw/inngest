# CEL and SQL compatibility baseline

**Phase:** 1, partial Go reference increment

**Worktree base:** `a4592cde49cab4c4a1afc5488cf39345514d5ed7` (`rust-core/01-compatibility-baseline`)

**Scope:** versioned CEL and filter-conversion vectors; no runtime changes

## Owners and boundaries

- [`expressions.EvaluateBoolean`](../../../../../pkg/expressions/expressions.go)
  and `NewBooleanEvaluator` own CEL evaluation. `Data` and the evaluator's
  partial activation handle absent paths; the CEL program applies the
  repository's null, numeric, truthy, and custom-function behavior.
- [`run.ExpressionHandler`](../../../../../pkg/run/cel.go) owns event matching
  and conversion to SQL expressions. The SQLite and PostgreSQL filter
  converters are in [`cel_sql.go`](../../../../../pkg/run/cel_sql.go); the CQRS
  manager selects a dialect converter through the DB adapter.
- Existing evaluator tests include `TestNegatedPresence`,
  `TestNullTypeHandling`, and `TestTruthyLogicalCoercion` in
  `pkg/expressions/unknown_test.go`; existing SQL converter tables are in
  `pkg/run/cel_test.go`.

## Portable fixtures

- [`cel-v1.json`](../../../../../pkg/expressions/testdata/compatibility/cel-v1.json)
  is consumed by `TestCELCompatibilityFixture` at the evaluator owner. It covers
  missing versus explicit nested null, JSON number versus CEL integer
  comparison, truthy `||`, `uppercase`, a partial unknown, and compile and
  evaluation error stages.
- [`cel-sql-v1.json`](../../../../../pkg/run/testdata/compatibility/cel-sql-v1.json)
  is consumed by `TestCELSQLCompatibilityFixture` at event matching and SQL
  conversion. It covers an event-name no-match, nested numeric comparison,
  boolean and JSON-null conversion, and a predicate that matches in CEL but is
  unsupported by SQL lowering. Supported cases pin prepared `WHERE` fragments
  and bound arguments for SQLite and PostgreSQL.

Both files carry format versions, provenance, and normalization notes. Inputs
are literal JSON: omitted keys are absent, explicit `null` is present, and JSON
numeric representation is normalized across consumers. There are no
time-dependent values or generated IDs; the fixtures state how such values
must be represented if added later. SQL argument ordering and fragments are
literal expected values. Numeric Go widths are normalized by JSON encoding;
strings and booleans remain typed. The run test renders a query only to expose
the prepared filter fragment and arguments; it does not execute a DB query.

## Test-audit record

1. **Contract protected:** a CEL consumer in another language must preserve
   these event-input results, missing/null handling, and custom behavior. The
   SQL owner must preserve which predicates are supported and the
   SQLite/Postgres filter fragments and bound values emitted for supported
   predicates.
2. **Credible regressions:** a port could treat missing as explicit null,
   compare JSON numbers differently from CEL literals, lose the current
   truthiness or custom-function semantics, resolve partial unknowns
   differently, shift compile/evaluation failures, or emit different JSON
   paths, casts, null handling, placeholder order, or typed bound values.
3. **Why existing coverage did not subsume this:** Go-native CEL tables already
   cover much of the evaluator behavior and Goqu-expression tables cover many
   conversions, but neither is a versioned fixture a Rust implementation can
   consume. These tests retain the same observable answers in a portable format;
   they are a cross-language compatibility contract, not AST-shape checks.
4. **Production seam:** none. Tests call the evaluator and `ExpressionHandler`
   owners. No production exports, flags, wrappers, or injection hooks were
   added.

## Validation and benchmark

The required Go 1.26.4 toolchain was used:

```sh
GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test ./pkg/expressions ./pkg/run -count=1
GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-t3-c12a454d GOMAXPROCS=2 GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 -race ./pkg/expressions -run '^TestCELCompatibilityFixture$' -count=1
GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-t3-c12a454d GOMAXPROCS=2 GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 -race ./pkg/run -run '^TestCELSQLCompatibilityFixture$' -count=1
GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-t3-c12a454d GOMAXPROCS=1 GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test -p=1 ./pkg/expressions -run '^$' -bench '^BenchmarkCELCompatibilityCorpus$' -benchmem -benchtime=100ms -count=10
```

The package and focused race commands passed during initial validation. The
renamed benchmark command was rerun under the same environment, completing ten
samples per path. The first parallel race attempt used the nearly full 30 GB
`/tmp` volume and failed while compiling dependencies;
the focused race commands passed when run sequentially with their Go temporary
directory under `/home/ashutosh/.cache`. The benchmark used Go 1.26.4, Linux/amd64,
`GOMAXPROCS=1`, and an AMD Ryzen 9 5950X 16-Core Processor. Each row summarizes
ten 100 ms samples. The first benchmark path creates a unique source by adding
numeric literals to the fixture expression on each iteration; source
uniquification is included in the measurement and bypasses `NewExpressionEvaluator`'s
source-keyed evaluator cache. `exprCompiler.Parse` calls the vendored
`cachingCompiler.Parse`, which lifts numeric literals before its parser-cache
lookup; these variants therefore reuse a normalized CEL AST. This path measures
evaluator/program construction with cached parsing, not cold parsing or cold
full compilation. Current results are below.

| Workload path | Min / median / max (ns/op) | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Evaluator/program construction with cached parsing (unique source) | 35,006 / 36,401 / 64,885 | 29,904–30,175 | 426 |
| Cached program evaluation | 7,561 / 7,791 / 8,627 | 3,592 | 120 |
| Cache lookup plus evaluation | 7,764 / 7,944 / 8,789 | 3,672 | 121 |

These are Go microbenchmark observations, not p95, end-to-end bridge results,
or calibrated migration gates. No SQLite or PostgreSQL server was used. A Rust
consumer has not yet run these fixtures, so this increment does not establish
cross-language parity.

## Remaining Phase 1 Go baseline work

This partial Go oracle does not complete Phase 1. Remaining Go baseline work
includes running the rendered SQL behavior against supported SQLite/PostgreSQL
versions, broader CEL/SQL operator coverage, SDK-response replay, queue/state
persistence, lifecycle/fault and recovery coverage, initial Go performance
baselines, and owner-confirmed SDK/version policy. These establish current Go
behavior before Rust parity work.

## Later Rust parity and bridge work

Rust fixture consumption and differential parity belong to Phase 4, after the
Phase 2 bridge spike and Phase 3 opcode slice. Compare FFI and process bridge
prototypes on Linux amd64/arm64 first; matched Go/Rust/bridge performance follows
as the Rust slices land and is calibrated before rollout. macOS/Windows parity
remains deferred until before broader release or Go retirement, and cloud parity
is a separate gate. No Rust workspace, crate, bridge, runtime change, commit, or
PR is selected or included in this increment.
