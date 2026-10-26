# SDK request V1 compatibility baseline

This corpus pins the SDK serialization behavior of `driver.MarshalV1` at Inngest
revision `3624c37d5285805d2a95efd0715e8913d5f81ae8`. Its production owners are
`pkg/execution/driver/driver.go` and `request.go`; canonicalization uses the pinned
`github.com/gowebpki/jcs v1.0.0` dependency. It covers SDK request serialization
only. HTTP transport, signing, SDK execution, and the rest of a Rust migration
are outside this baseline.

Expectations were authored from that revision's wire contract, with an independent
JSON writer used to order object keys by UTF-16 code units and emit UTF-8. They
were then checked byte-for-byte against the unchanged Go implementation by
`TestMarshalV1Compatibility`. Neither the test nor a fixture-update mode generates
expected bytes with `MarshalV1`, `SDKRequest`, or JCS. These fixtures record the
current behavior, including precision loss and raw-input prefix recognition;
they do not propose new behavior.

## Portable format

`cases.json` is the language-neutral entry point. `common` provides function ID,
run ID, environment, step ID, maximum attempts, and queue reference for every
case. Each case's `input` supplies loader responses, metadata, and invocation
arguments. Omitted numeric/boolean/string arguments mean zero/false/empty.
`stack: null` means a nil stack, while `[]` means an allocated empty stack.

Event array entries and step map values are **strings containing raw JSON**.
Preserve their exact text until feeding them to the serializer: reparsing them
early loses key order, leading whitespace, and numeric literals. Defer entries
use status names from `DeferStatus`; `fn_slug`, `hashed_id`, and `meta` are loader
metadata that must not appear on the wire. `defers_error` selects a gRPC
Unimplemented or Unavailable response with message `fixture defers failure`.
`steps_error`, when present, is an ordinary loader error. The above-threshold
case supplies it to detect accidental state loading.

`expected_file` contains the exact canonical UTF-8 response, with no BOM or
trailing newline. Compare bytes, not parsed objects. `expected_bytes` and
`expected_sha256` describe the expanded response for consumers in other
languages. Error cases instead carry the exact baseline Go diagnostic in
`expected_error` and expect no output bytes; another implementation may compare
its equivalent failure category rather than its language-specific error text.
The malformed-input cases have stable portable categories: invalid event JSON,
invalid stacked-step JSON, and duplicate object keys in stacked-step JSON.
The first fails event decoding, the second fails request JSON serialization,
and the third passes JSON serialization but fails JCS canonicalization. A port
may reject duplicate keys earlier; the portable contract is rejection with no
output bytes, not the Go error wording or the rejection stage. Each case uses
one small invalid payload with otherwise valid input, without injected errors.

Large strings use `recipes`: replace each token literally with `ascii` repeated
`count` times, in each raw event/step string and expected wire file. All recipes
here repeat the single ASCII byte `x`. Expand before parsing raw JSON; do not
serialize the expected template afterward. This fixed textual expansion is the
only runtime fixture construction. There is no oracle regeneration. The checked-in
corpus remains small while still exercising real multi-megabyte requests.

## Contracts and boundaries

- Empty nil and allocated stacks serialize as `[]`; absent and null event users
  become `{}`. Request version `-1`, empty defer/step maps, and omitted dispatch
  identifiers are pinned.
- Replay includes successful and failed steps in stack order, completed altered
  input, and unfinished altered input. Ordinary out-of-stack results disappear.
  Leading whitespace or a key before `input` prevents unfinished input inclusion
  at this revision. The fixture also pins attempts, stack index, dispatch IDs,
  immediate execution control, and a two-event batch.
- Nested nulls, fractions, negative zero, HTML characters, composed/decomposed
  Unicode, and supplementary Unicode keys exercise canonical bytes. Event
  `9007199254740993` is decoded through binary64; both event and raw step numbers
  reach canonical output as `9007199254740992`. Key ordering places U+1F600 before
  U+E000 by UTF-16 code units. Replay includes 16,384 payload bytes.
- Missing stack state fails; out-of-stack state alone does not. Only `after_run`
  defers are abortable, including explicit false for unknown, scheduled, aborted,
  and rejected statuses. Unimplemented metadata loading behaves as empty defers;
  Unavailable remains an error.
- Malformed event and raw stacked-step syntax fail without output. Duplicate
  keys in raw stacked-step data are preserved until JCS rejects them.
- Coarse state-size cases pin 4,193,279 / 4,193,280 / 4,193,281 bytes. The first
  two inline steps; the last skips state loading, retains events, and sets both
  `use_api` fields.
- Serialized-body cases pin **pre-JCS** sizes 4,194,303 / 4,194,304 / 4,194,305.
  For these inputs, Go JSON has 457 fixed bytes plus the recipe's `x` count.
  Counts are fixed at 4,193,846 / 4,193,847 / 4,193,848. The final `<` expands to
  six bytes (`\u003c`) before JCS and one afterward. Below/at canonical output
  therefore has 4,194,298 / 4,194,299 bytes. Above falls back even though its
  hypothetical canonical output would fit: it clears events and steps, preserves
  the first event and stack, and sets both `use_api` fields.
- A separate 2,097,152-byte event payload exceeds the body limit through event
  duplication alone. Fallback retains that first event inline while clearing
  the batch. The resulting canonical request has 2,097,543 bytes.

## Validation and benchmark use

Run from the repository root with the repository's Go 1.26.4 toolchain:

```sh
go test ./pkg/execution/driver -run 'Test(MarshalV1Compatibility|Stack)$' -count=1
go test ./pkg/execution/driver ./pkg/execution/driver/connectdriver ./pkg/execution/driver/httpv2 ./pkg/execution/driver/mockdriver -count=1
GOMAXPROCS=2 go test ./pkg/execution/driver -run '^$' -bench 'Benchmark(MarshalV1|JCS)Compatibility' -benchmem -benchtime=300ms -count=5
```

Benchmarks reuse four corpus cases: replay, coarse state-size fallback, actual
body-size fallback, and duplicated-event fallback. Fixture loading, recipe
expansion, metadata/context setup, and raw payload allocation occur outside the
timed region. Every Marshal iteration recreates the loader's step map because
`MarshalV1` deletes stack entries; that map-copy cost is included. Raw payload
bytes are immutable and reused. For `state_size_above`, the map copy is
unnecessary fixture reset overhead: this path never loads steps. Its recorded
Marshal numbers include that overhead and do not isolate the pure skipped-state
cost. The benchmark code and recorded measurements are preserved.
JCS benchmarks separately parse and canonicalize
the pinned **already canonical final wire**, excluding all Marshal/loader work;
they do not measure the discarded oversized intermediate request. Throughput is
reported only for JCS, using final wire length. Go benchmark timings are means
in ns/op, with allocation counts; they are not p95 latency measurements.

### Independent review

Sol authored this corpus and Luna independently reviewed it with no blockers.
The findings on malformed-case clarity and the state-size benchmark's
unnecessary fixture-reset overhead label were resolved. The malformed vectors
state their portable rejection categories, and the benchmark notes identify
the included map-copy cost.

### Recorded baseline (2026-10-26)

Environment: Linux amd64, kernel `5.15.0-160-generic`, AMD Ryzen 9 5950X
(16 cores / 32 logical CPUs), Go `go1.26.4 linux/amd64`, `GOAMD64=v1`,
`CGO_ENABLED=1`, and `GOMAXPROCS=2`. The shell's default Go is 1.18.1, so the
commands above were executed with
`/home/ashutosh/.cache/go1.26.4/go/bin/go` substituted for `go`.

The focused test command passed (0.254s). The complete non-HTTP-driver package
command, with `GOMAXPROCS=2`, passed: driver 0.250s, connectdriver 0.016s, httpv2
0.109s; mockdriver has no tests. No full repository suite or HTTP-driver tests
were run for this bounded change.

The additional focused race check also passed (3.187s):

```sh
GOMAXPROCS=2 /home/ashutosh/.cache/go1.26.4/go/bin/go test -race ./pkg/execution/driver -run 'Test(MarshalV1Compatibility|Stack)$' -count=1
```

After adding the three malformed-input cases, the focused test command passed
again (0.222s), as did its focused race check (3.264s), both with `GOMAXPROCS=2`
and the same Go 1.26.4 toolchain. All tests in `./pkg/execution/driver` also
passed (0.252s). The benchmark loop is unchanged; the recorded
benchmark results below were not rerun or replaced for this fixture addition.

The benchmark command above completed successfully (19.688s). Each result below
is the median of **five run-level ns/op means**, followed by their minimum and
maximum. These ranges describe run-to-run variation, not latency percentiles.
Raw output from this execution is at `/tmp/inngest-sdk-request-v1-bench.txt` in
the implementation workspace; that temporary log is not part of the corpus.

- Marshal replay: **367,440 ns/op**, range 327,842–399,679;
  287,256–287,440 B/op; 811 allocs/op.
- Marshal state-size fallback (includes unnecessary fixture reset overhead;
  not pure skipped-state cost): **19,259 ns/op**, range 18,383–22,398;
  10,158 B/op; 264 allocs/op.
- Marshal body-size fallback: **23,671,447 ns/op**, range 21,302,867–25,096,259;
  4,212,768–6,148,646 B/op; 253–255 allocs/op.
- Marshal duplicated-event fallback: **41,412,117 ns/op**, range
  38,382,960–48,911,787; 52,756,298–52,756,314 B/op; 362 allocs/op.
- JCS replay: **223,101 ns/op**, range 203,894–273,242;
  260,488–260,489 B/op; 646 allocs/op.
- JCS state-size fallback wire: **13,196 ns/op**, range 11,829–14,306;
  7,536 B/op; 222 allocs/op.
- JCS body-size fallback wire: **9,713 ns/op**, range 9,506–10,920;
  6,640 B/op; 191 allocs/op.
- JCS duplicated-event fallback wire: **22,455,983 ns/op**, range
  19,046,531–24,231,918; 29,609,010–29,609,024 B/op; 266 allocs/op.

Pool reuse and garbage collection can affect allocation and timing samples,
especially the discarded oversized body. These measurements establish an
environment-specific comparison point; they are not a performance acceptance
threshold or an end-to-end SDK latency claim.
