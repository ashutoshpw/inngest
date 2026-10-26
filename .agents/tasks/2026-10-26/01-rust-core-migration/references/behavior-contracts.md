# Behavior and persistence contracts

These are observable contracts to capture as fixtures and preserve through the migration. The list is grounded in current source and package documentation; it is not a claim that all behavior already has complete test coverage.

## SDK and serialization compatibility

- [`driver.MarshalV1`](../../../../../pkg/execution/driver/driver.go) builds a versioned SDK request and canonicalizes data with JCS. [`state/README.md`](../../../../../pkg/execution/state/README.md) documents request/hash versioning and replay compatibility; changing inputs or step hashes can break replay of existing runs.
- Preserve dynamic JSON shape, string opcode enums, and event timestamp representation. [`event.Event.Map`](../../../../../pkg/event/event.go) emits `ts` as `float64` to match JSON/JavaScript behavior.
- [`httpdriver.ParseGenerator`](../../../../../pkg/execution/driver/httpdriver/parse.go) accepts one opcode object or an array; an empty array becomes `OpcodeNone`. A 206 response is a partial generator response, not an ordinary completed function result (see [`driver_response.go`](../../../../../pkg/execution/state/driver_response.go)).
- The [SDK request V1 corpus](../../../../../pkg/execution/driver/testdata/sdk-request-v1/README.md) pins request bytes and replay-shaped step inputs, but does not cover paired SDK-response replay. After owners confirm the minimum and maintained SDK versions, extend fixtures across that agreed range with hash versions, step inputs/errors, opcode ordering, null/missing values, and retry responses. Never assume changing request encoding is harmless.

## CEL and event matching

- [`pkg/expressions`](../../../../../pkg/expressions) contains custom functions, missing-attribute handling, unknown/partial evaluation, numeric and truthy coercion, and expression-driven field selection. [`pkg/run/cel.go`](../../../../../pkg/run/cel.go) also lowers filters to SQL. A Rust CEL implementation must match both runtime evaluation and SQL filter behavior; run a parity spike before choosing an engine.
- Fixtures must cover custom functions, missing nested attributes, null and mixed numeric types, partial evaluation, boolean/truthy coercion, error cases, and the corresponding SQL filter output.

## State, queue, and execution ordering

- Preserve the compact queue item JSON shape, IDs, Redis key names, TTLs, and Lua atomic operations used by [`redis_state`](../../../../../pkg/execution/state/redis_state). Keep SQLite/Postgres schemas and current Goose migration behavior compatible.
- Preserve these ordering and recovery properties: state is written before enqueue; output is persisted before discovery recovery; pending steps are saved before fan-out; parallel discovery uses coalesced job IDs; priority wait/defer operations drain before finalization; lazy defers stay out of pending-step accounting; finalization loads defers before state deletion; semaphore releases occur on completion; claims deduplicate effects.
- Shutdown must persist outcomes and schedule children. Failed finalization must be retried. Add fault injection around every boundary between persisted state, enqueue, output save, discovery, claim, and finalization. Dedupe is required, but the system does not promise exactly-once execution.
- Preserve schedule/execute semantics and the current queue dispatch/retry behavior in [`executor.go`](../../../../../pkg/execution/executor/executor.go) and [`service.go`](../../../../../pkg/execution/executor/service.go). The broad `execution.Executor` interface is not the proposed migration API.

## Defers, streaming, and Connect

- [`docs/defer.md`](../../../../../docs/defer.md) records lazy defer add/abort behavior, Redis atomic writes, size/count caps, finalization events, and a missing `run_type = Defer` end-to-end test blocked on Go SDK support. Preserve those semantics and track the SDK dependency separately.
- [`realtime/broadcaster.md`](../../../../../pkg/execution/realtime/docs/broadcaster.md) and [`durable-endpoints/streaming.md`](../../../../../docs/durable-endpoints/streaming.md) describe Redis pub/sub streaming of raw bytes without replay or buffering. Preserve those boundaries unless a separate protocol change is approved.
- Keep Go Connect gateways initially. [`connect.proto`](../../../../../proto/connect/v1/connect.proto) defines the protocol; [plan 003](../../../../../docs/plans/003-connect-gateway-lifecycle-controller.org) tracks worker ACK acceptance/forwarding, while executor ACK is best-effort. If gateways move later, require a dedicated protocol and lifecycle parity gate.
