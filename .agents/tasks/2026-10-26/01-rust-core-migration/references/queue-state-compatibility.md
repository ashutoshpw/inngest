# Queue and run-state persistence compatibility baseline

**Phase:** 1, partial Go reference increment (stack layer 3)

**Worktree base:** `8ebc428587cc11220e86d6fbbd5ab7ffc3bf5fef` (`t3/cel-sql-compat-fixtures`)

**Scope:** versioned persisted-value fixtures at the queue item and initial run-state owners; no runtime changes

## Owners and boundaries

- [`queue.EnqueueItem`](../../../../../pkg/execution/state/redis_state/queue.go)
  serializes the finalized queue item into the Redis queue-item hash.
  [`queue.LoadQueueItem`](../../../../../pkg/execution/state/redis_state/queue.go)
  reads that hash value, decodes the item, and restores `Data.JobID` from the
  outer ID. [`queue.Item.UnmarshalJSON`](../../../../../pkg/execution/queue/item.go)
  selects concrete payload types by queue kind.
- `shardedMgr.New` and `shardedMgr.Load` in
  [`redis_state.go`](../../../../../pkg/execution/state/redis_state/redis_state.go)
  own initial run-state writes and reconstruction. `New` stores the event batch,
  metadata fields, pre-memoized action results, stack entries, and action inputs;
  `Load` reads those values back into `state.State`.

## Queue item fixture

[`queue-item-v1.json`](../../../../../pkg/execution/state/redis_state/testdata/compatibility/queue-item-v1.json)
is consumed by `TestQueueItemPersistenceCompatibility`. The test uses fixed IDs,
a fixed fake queue clock, and passthrough IDs, then exercises the real
`EnqueueItem -> Redis queue-item hash -> LoadQueueItem` path. It compares the
actual stored value to literal fixture JSON structurally, so Go object key order
is not treated as a byte contract. It also checks the reloaded identifier,
generation, timestamps, compact data fields, restored `JobID`, and concrete
payload types.

The local expected persisted shape decodes `Data.PriorityFactor` and
`Data.ParallelCoalesceKey` as `*int64` and `*string`, independently of the
production queue type. The test compares both pointers after `LoadQueueItem`.
The first case expects `3` and `parallel-batch-1`; the other two persisted
cases omit both `omitempty` fields and decode to nil, which the same assertions
check.

The three cases cover `start` and `edge` jobs with the edge payload, plus the
`fp` function-pause payload. They pin durable queue IDs, generation IDs,
workflow versions, batch/event IDs, an attempt/version-bearing identifier,
compact edge payload keys, an optional metadata `null`, and the distinction
between a populated, empty, and nil `eventIDs` list. The fixture has no generated
values, so no normalization recipe is needed.

## Initial run-state fixture

[`run-state-v1.json`](../../../../../pkg/execution/state/redis_state/testdata/compatibility/run-state-v1.json)
is consumed by `TestRunStatePersistenceCompatibility`. It calls the real
`New`, reads the resulting event value, selected metadata hash fields, action
results, action inputs, and stack from miniredis, then calls the real `Load` and
checks the reconstructed state. It pins the run identifier and workflow
version, request version, status and metadata version, scheduled timestamp,
context, representative event JSON, memoized result, stack ID, and step input.
The metadata identifier and context are JSON text in Redis hash fields; the
test parses them structurally while comparing scalar hash values as strings.

This is an initial-state write/load witness, not a process-restart or recovery
witness. Shutdown/restart, response writes, defer state, recovery, and real
Redis/Valkey server behavior remain open baseline work. These tests exercise
Lua through miniredis and make no claim about production-server Lua atomicity.

## Test-audit record

1. **Contract protected:** queue consumers in another language must read the
   current persisted queue JSON, preserve durable IDs and dispatch generation,
   and recover kind-specific payloads. Run-state consumers must interpret the
   representative initialization values and version metadata written by the
   current owner.
2. **Credible regressions:** renaming compact JSON fields, dropping generation
   or workflow-version data, changing omitted versus `null` versus empty lists,
   losing a generated payload type, failing to restore `JobID`, or changing the
   run-state request/metadata version or stored event/action values would fail
   the fixtures.
3. **Why existing coverage did not subsume this:** `TestLoadQueueItem` checks
   only the loaded ID. `TestQueueEnqueueItem` compares a Go item with a helper
   that decodes the same Go storage format and primarily covers enqueue and
   index behavior. Queue protobuf conversion tests cover a separate transport
   boundary. Existing run-state tests exercise many APIs, but there was no
   versioned literal fixture across `New`, actual stored Redis values, and
   `Load`.
4. **Production seam:** none. Both tests call existing storage owners with
   miniredis; no serializer copy, test-only production API, wrapper, or flag was
   added.

## Validation and benchmark

The required Go 1.26.4 toolchain was used on Linux/amd64. The default `/tmp`
volume had only 785 MB free at the start, so Go temporary files were placed in
`/home/ashutosh/.cache/go-tmp-inngest-t3-67fb7e0a` and parallelism was bounded.

Focused tests and the targeted race run passed:

```sh
GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-t3-67fb7e0a GOMAXPROCS=2 GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 ./pkg/execution/state/redis_state -run '^(TestQueueItemPersistenceCompatibility|TestRunStatePersistenceCompatibility|TestQueueEnqueueItem|TestLoadQueueItem|TestQueueScavenge)$' -count=1
GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-t3-67fb7e0a GOMAXPROCS=2 GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 -race ./pkg/execution/state/redis_state -run '^(TestQueueItemPersistenceCompatibility|TestRunStatePersistenceCompatibility)$' -count=1
git diff --check
```

The new benchmark separates JSON marshal plus typed unmarshal from enqueue plus
load against miniredis. Ten 100 ms samples used Go 1.26.4, Linux/amd64,
`GOMAXPROCS=1`, and an AMD Ryzen 9 5950X 16-Core Processor:

| Path | Min / median / max (ns/op) | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| JSON marshal plus typed unmarshal | 22,071 / 22,814 / 25,539 | 5,168 | 70 |
| Miniredis enqueue plus `LoadQueueItem` | 246,169 / 271,032 / 280,797 | 270,254–270,483 | 1,238 |

The second path includes miniredis and the queue Lua script. It is not Redis or
Valkey server latency, a production throughput result, or a p95 measurement.
No full package suite or real Redis/Valkey integration test was run.

## Review finding and revalidation

Sol-high review found that the queue compatibility test did not assert the
loaded `Data.PriorityFactor` or `Data.ParallelCoalesceKey`, even though the
first persisted literal contains both. The local expected shape and loaded
pointer assertions above fix that gap; no production code changed.

After the fix, the focused queue and run-state tests and targeted race run
passed with Go 1.26.4:

```sh
GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-t3-67fb7e0a GOMAXPROCS=2 GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 ./pkg/execution/state/redis_state -run '^(TestQueueItemPersistenceCompatibility|TestRunStatePersistenceCompatibility)$' -count=1
GOTMPDIR=/home/ashutosh/.cache/go-tmp-inngest-t3-67fb7e0a GOMAXPROCS=2 GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 -race ./pkg/execution/state/redis_state -run '^(TestQueueItemPersistenceCompatibility|TestRunStatePersistenceCompatibility)$' -count=1
git diff --check
```

A mutation probe temporarily omitted the `PriorityFactor` restore assignment
in `Item.UnmarshalJSON`. Because that same unmarshaler decodes the enqueue
input, the probe temporarily restored the two input pointer values from a
plain shape decoded from the literal input fixture; with that setup, the test
failed at the new loaded-pointer equality (`*int64` expected, nil actual). The
temporary changes to `item.go` and the test were both restored byte-for-byte
(SHA-256 `9ffa2106037c290a4f4793d29a62cd688122693cdaa1b2c69c5fcc1ebd20a5c2`
and `60d4427fe5a8931562400a66653c88816c3703bb8720043256ddcb50d34dfd63`,
respectively). This is focused proof only; no full package suite or full Phase
1 completion is claimed.

## Review and remaining work

The parent accepted this partial stack layer 3 after Sol-high's independent
review found one P2: the loaded queue item did not assert `PriorityFactor` or
`ParallelCoalesceKey`. Luna added literal nonnil pointer expectations and checks
for the omitted/nil cases, then passed focused tests, the targeted race run, and
a mutation probe that failed at the loaded-pointer assertion. No production
code changed. Phase 1 remains partial; Rust fixture consumption and parity
remain later work.

A separate process-restart/recovery witness is under independent review in the
different worktree on branch `t3/recovery-replay-witness`. It is not included in
this layer. Queue/state recovery, broader lifecycle fault cases, and real
Redis/Valkey behavior remain open baseline work.
