# Phase 1 SDK and deployment coverage

Snapshot: 2026-10-26. This records configured coverage observed in this checkout,
not successful CI runs or a supported-version policy. No runtime tests or builds
were run for this artifact. The [migration plan](../plan.md) requires agreed
contracts and targets before phase 2; the initial bridge validation targets
agreed below do not settle the remaining support-policy questions.

## Initial bridge scope decision

User decision, 2026-10-26: "Prioritize Linux amd64/arm64 first". First bridge
validation targets Linux amd64 and arm64 while preserving currently implemented
SDK and request-version behavior, including legacy replay behavior. This agrees
the initial validation targets, not minimum supported SDK versions, language
support ranges, or cloud service parity/ownership obligations; those remain
owner-unconfirmed.

macOS and Windows validation is deferred to later parity gates, not removed
support. Full validation of all six configured release targets is not a
prerequisite for the initial Linux spike. Deferred platform parity obligations
must be resolved and validated before broader release or Go retirement. Cloud
parity remains a separate rollout gate, with its required scope and ownership
still to be confirmed.

## Observed matrix

| Surface | Repository evidence | Boundary of the evidence |
| --- | --- | --- |
| CLI release binaries | [GoReleaser](../../../../../.goreleaser.yml): Linux, Windows, Darwin × amd64, arm64; `CGO_ENABLED=0`, main `./cmd/` | Six configured build targets; does not prove runtime parity on all six. An FFI choice would change the current no-CGo packaging assumption. |
| Container release | [GoReleaser](../../../../../.goreleaser.yml), [Dockerfile](../../../../../Dockerfile.goreleaser): Linux amd64 and arm64, Buildx images and combined manifests | No Windows/macOS containers or runtime architecture test matrix established here. |
| Go unit/race | [Go workflow](../../../../../.github/workflows/go.yaml): `depot-ubuntu-22.04-4`, SQLite/Postgres, `TEST_MODE=true`, `-race -count=1`; excludes `./tests` packages | Linux runner coverage, not a release OS/architecture cross-product. Workflow also runs lint and Linux codegen cleanliness (`make gen protobuf` via Nix). |
| JS SDK E2E | [E2E workflow](../../../../../.github/workflows/e2e.yml): `depot-ubuntu-22.04`, key queues false/true; Next SDK app plus dev server and `go test ./tests` | No explicit Postgres axis in this job. Node comes from [.tool-versions](../../../../../.tool-versions): 22.16.0; pnpm 10.18.2. |
| JS SDK inputs | [package.json](../../../../../tests/js/package.json): `inngest` = `3.11.1-pr-411.4`; `inngest-v4` = `npm:inngest@4.3.0`; [v3 client](../../../../../tests/js/src/inngest/client.ts), [v4 client](../../../../../tests/js/src/inngest/client_v4.ts) import those packages | Observed test pins/imports; no minimum, maximum, maintained range, or complete v3/v4 feature parity inferred. |
| Go SDK E2E | [go.mod](../../../../../go.mod): `github.com/inngest/inngestgo v0.16.5`; [harness](../../../../../tests/golang/golang_sdk_util.go) imports it; [E2E workflow](../../../../../.github/workflows/e2e.yml): Linux, SQLite/Postgres, key queues false/true, five test shards | One pinned SDK input; no supported-version range inferred. |
| Batch/API/execution | [E2E workflow](../../../../../.github/workflows/e2e.yml): Linux; batch/API use SQLite/Postgres × key queues false/true; batch also toggles split-batch partitions; execution toggles key queues | Separate runtime suites; not additional SDK-version or platform coverage. |
| npm CLI installation | [npm workflow](../../../../../.github/workflows/npm_test.yml): Node 18/20 × Ubuntu/macOS/Windows; build npm wrapper, postinstall, `bin/inngest version` using previous release tag | CLI download/install smoke coverage, triggered by `npm/**`; not server execution or SDK parity, nor an architecture matrix. |

## Request/hash compatibility is a separate contract

The [state README](../../../../../pkg/execution/state/README.md) describes `-1`
as unknown until the first SDK response, legacy `0` as TS v1/v2 hashing, and `1`
as the documented `stepID:index` format. Its historical field names should be
read alongside current [metadata](../../../../../pkg/execution/state/v2/state_metadata.go):
`Config.RequestVersion` carries hashing style; comments identify TS v3, Go,
Rust, Elixir and Java as sharing style 1. Those comments establish a mechanism,
not tested SDK versions or a support commitment for those languages. The README's
historical exactly-once wording is not a migration guarantee.

Independent implementation evidence:

- [MarshalV1](../../../../../pkg/execution/driver/driver.go) puts persisted
  `md.Config.RequestVersion` into the request version field and canonicalizes JSON.
- [HTTP driver](../../../../../pkg/execution/driver/httpdriver/httpdriver.go)
  sends `x-inngest-req-version` when a version is provided, and parses the
  response header using `strconv.Atoi` with its error ignored (missing/invalid
  values therefore become 0). [HTTP v2](../../../../../pkg/execution/driver/httpv2/httpv2.go)
  also exchanges the header through [header helpers](../../../../../pkg/headers/headers.go).
- [Connect driver](../../../../../pkg/execution/driver/connectdriver/connectdriver.go)
  carries the response request version; [executor](../../../../../pkg/execution/executor/executor.go)
  persists it when handling a generator response with unknown stored version.
  Preserve these transport and persistence paths independently of dependency pins.

## Bridge validation and later parity gates

1. Start the bridge spike with the agreed Linux amd64/arm64 targets and preserve
   currently implemented SDK/request-version behavior. Owners must still confirm
   supported SDK languages/version ranges (including minimum versions), the full
   legacy replay obligations, later deployment OS/architecture/runtime acceptance
   combinations, and cloud service parity/ownership. These remaining policy
   questions are **owner-unconfirmed**; this OSS snapshot cannot close them.
   Confirm later acceptance coverage for the six configured release targets and
   two container targets before broader release or Go retirement; do not make
   full six-target validation a prerequisite for the initial Linux spike.
2. Review a portable request/response and replay corpus, including unknown,
   legacy and current hash styles, absent/invalid headers, canonical bytes,
   memoized steps and stored versions. Dependency pins cannot substitute for
   those witnesses. Cover opcode/lifecycle, CEL, queue/state and failure ordering
   per [baseline discovery](baseline-discovery.md).
3. Capture matched Go performance baselines and environment metadata; calibrate
   the plan's proposed latency/throughput/memory limits before using them as
   acceptance thresholds. No measurements are claimed here.
4. Then compare FFI and process prototypes first on Linux amd64/arm64: build/link
   or bundle/install behavior, panic/cancellation, ownership/memory, startup and
   end-to-end cost. Extend validation to deferred macOS/Windows targets and other
   confirmed parity obligations before broader release or Go retirement; cloud
   parity is a separate rollout gate. Preserve Go-readable state and a single
   effect owner. Rust being installed does not select a Cargo workspace, crates
   or bridge design. See [validation/rollout](validation-rollout.md) for later
   activation gates.

## Local validation entry points (not executed)

Use the existing Go 1.26.4 binary at
`/home/ashutosh/.cache/go1.26.4/go/bin/go`; [go.mod](../../../../../go.mod)
requires 1.26.4. Put its directory first on `PATH` for commands that invoke `go`.
Focused entry points from the source packages:

```sh
export PATH="/home/ashutosh/.cache/go1.26.4/go/bin:$PATH"
go test ./pkg/execution/driver/... ./pkg/execution/state/... -count=1
go test ./pkg/expressions ./pkg/run ./pkg/execution/executor -count=1
go test ./tests/execution/executor -count=1
go test ./pkg/db/... -count=1
TEST_DATABASE=postgres go test ./pkg/db/... -count=1
```

Go module/vendor inputs and a working native toolchain for race builds are
required. [Executor integration helpers](../../../../../tests/execution/executor/helpers_test.go)
use test SQLite and in-memory Redis; real-server compatibility suites use
Docker/testcontainers via the [Valkey helper](../../../../../tests/execution/queue/helper/valkey.go).
Postgres adapter tests start a test container and require Docker; inspect the
[DB test setup](../../../../../pkg/db/adapter_integration_test.go) before running
other database suites, whose setup may differ.

For SDK E2E, reproduce the [workflow](../../../../../.github/workflows/e2e.yml)
setup: build `go build -cover -o ./inngest-bin ./cmd`, create `GOCOVERDIR`, start
`inngest-bin dev --no-discovery --no-poll`, wait for port 8288 health, and run
`go test ./tests/golang -count=1` with the workflow's API/event/signing/dev/test
and key-queue environment. Postgres variants require Postgres 17 (CI uses Docker)
and the explicit `--postgres-uri`. JS additionally requires Node 22.16.0,
pnpm 10.18.2, `make build-ui`, `pnpm install --frozen-lockfile` and `pnpm dev`
in `tests/js`, SDK port 3000, then `go test ./tests` with the TS job environment.
These are separate variants; copy the exact environment from the relevant job.

[Makefile](../../../../../Makefile) provides broader `make test` (unit/race plus
lint), `make lint`, and `make e2e` (`tests.sh`) entry points. CI wrappers additionally
need gotestsum 1.13.0 / `go install tool`, and codegen needs the Nix environment.
No command above establishes Rust compatibility until a reviewed corpus and a
selected Rust consumer exist.
