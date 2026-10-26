# Go service baseline for the bounded Linux bridge gate

**Evidence snapshot:** 2026-10-26, clean worktree `t3-22968110`, commit
`f2fcebce2ec7cb1528f04114d4a9fb43930ad765` (PR4 snapshot). The recorded Go
stdout and exit files were saved under
`/home/ashutosh/.cache/inngest-phase1-gotmp/logs/`. This reference preserves the
results and runnable selections so the conclusion does not depend on that
local cache.

All requested service families and the separately labeled extra runs completed
with exit code 0. This is the bounded Go service-evidence leg for the pure Linux
bridge gate. It does not establish Rust parity. Broad Phase 1 remains partial;
the parent also accepted independent Luna review of the exact-slice oracle,
satisfying the bounded entry gate. Phase 2 remains uncompleted and unselected.
The current sequencing and rollout boundaries are in the
[bounded bridge contract](opcode-bridge-contract.md) and
[validation and rollout plan](validation-rollout.md).

## Environment and service images

- Linux/amd64; Go 1.26.4 from
  `/home/ashutosh/.cache/go1.26.4/go/bin/go`; `GOTOOLCHAIN=local`.
- `GOMAXPROCS=2`, `TEST_MODE=true`, and both `GOTMPDIR` and `TMPDIR` set to
  `/home/ashutosh/.cache/inngest-phase1-gotmp`.
- Docker Engine 28.3.2 through `unix:///var/run/docker.sock`; Testcontainers
  for Go v0.42.0. Docker group access was supplied with
  `sudo -n -u ashutosh -g docker`.
- PostgreSQL used `postgres:17`, digest
  `sha256:b8faeff7712663c2773a190181e1246e37e364b887611c04658c28b44e2408fe`.
- Queue and state Lua compatibility tests used `valkey/valkey:8.0.1`, digest
  `sha256:c5d4f082b76d433a38274ebb38a43cff19829a062928bdcc45611135ef54e608`.
  The test helper pins `linux/amd64`; see the
  [Valkey helper](../../../../../tests/execution/queue/helper/valkey.go) and
  [test image constants](../../../../../tests/testutil/constants.go). The
  separate [recovery baseline](recovery-baseline.md) uses Valkey 8.0.3, so its
  server image must not be conflated with this 8.0.1 run.

The PostgreSQL fixtures start real PostgreSQL testcontainers through
[`StartPostgres`](../../../../../tests/testutil/postgres.go). `TEST_DATABASE`
selects SQLite or PostgreSQL for the DB and CQRS fixtures; the executor test
helpers construct SQLite explicitly in
[`tests/execution/executor/helpers_test.go`](../../../../../tests/execution/executor/helpers_test.go).
In the DB and CQRS helpers, `TEST_DATABASE=sqlite` explicitly selects the same
SQLite fallback branch used when `TEST_DATABASE` is unset.
Setting `TEST_DATABASE=postgres` on the queue or state-store invocation does
not turn its Valkey Lua server into PostgreSQL. The service-backed PostgreSQL
evidence in this run is the `pkg/db` integration suite and the selected CEL
manager tests, whose fixtures choose PostgreSQL through `TEST_DATABASE`; see
[`pkg/db` integration setup](../../../../../pkg/db/adapter_integration_test.go)
and [CQRS test setup](../../../../../pkg/cqrs/manager/cqrs_test.go).

## Reproducible commands and selections

The archived `.log` files contain verbose test output and the paired `.exit`
files contain exit codes, but they do not record the shell's original argv.
These commands reconstruct the package and top-level test selections shown in
that output. They use `-count=1` to force a fresh run and `-v` to retain the
logged test detail.

```sh
export PATH="/home/ashutosh/.cache/go1.26.4/go/bin:$PATH"
export GOTOOLCHAIN=local
export GOMAXPROCS=2
export TEST_MODE=true
export GOTMPDIR=/home/ashutosh/.cache/inngest-phase1-gotmp
export TMPDIR=/home/ashutosh/.cache/inngest-phase1-gotmp
```

Requested selections:

```sh
TEST_DATABASE=sqlite go test -v -count=1 ./pkg/db/...
TEST_DATABASE=postgres go test -v -count=1 ./pkg/db

TEST_DATABASE=sqlite go test -v -count=1 ./pkg/cqrs/manager \
  -run '^(TestCQRSGetSpanRunsCELJoinQualifiesTenantFilters|TestCQRSGetSpanRunsRejectsUnsupportedCELPredicates)$'
TEST_DATABASE=postgres go test -v -count=1 ./pkg/cqrs/manager \
  -run '^(TestCQRSGetSpanRunsCELJoinQualifiesTenantFilters|TestCQRSGetSpanRunsRejectsUnsupportedCELPredicates)$'

TEST_DATABASE=sqlite go test -v -count=1 ./tests/execution/queue \
  -run '^TestLuaCompatibility$'
TEST_DATABASE=sqlite go test -v -count=1 ./tests/execution/state_store \
  -run '^(TestUpdateMetadataIsFieldEmpty|TestStateStoreLuaCompatibility|TestSavePauseLuaCompatibility|TestDeletePauseLuaCompatibility|TestStateOperationsAcrossBackends)$'
TEST_DATABASE=sqlite go test -v -count=1 ./pkg/execution/executor ./tests/execution/executor \
  -run '^(TestCancelForceLifecycleHookFinalizesWhenMetadataMissing|TestOpcodePlanCompatibilityAtResponseHandler|TestHandleGeneratorResponse_CoalesceKeyBySDKRequestVersion|TestScheduleQueueShardNotFoundCleansFreshState|TestDeferFinalize|TestDeferAdd|TestDeferAbort|TestFinalizePublishesFnFinishedOnce|TestFinalizeRetriesFnFinishedAfterPublishFailure|TestFinalizeDeleteFailureDoesNotLoseFinishEffects|TestParallelPauseBackedOpsCoalesceDiscovery|TestSleepCompletionSaveRetries)$'
```

Additional PostgreSQL-environment runs, separate from the requested selections:

```sh
TEST_DATABASE=postgres go test -v -count=1 ./pkg/db/...
TEST_DATABASE=postgres go test -v -count=1 ./tests/execution/queue \
  -run '^TestLuaCompatibility$'
TEST_DATABASE=postgres go test -v -count=1 ./tests/execution/state_store \
  -run '^(TestUpdateMetadataIsFieldEmpty|TestStateStoreLuaCompatibility|TestSavePauseLuaCompatibility|TestDeletePauseLuaCompatibility|TestStateOperationsAcrossBackends)$'
TEST_DATABASE=postgres go test -v -count=1 ./pkg/execution/executor ./tests/execution/executor \
  -run '^(TestCancelForceLifecycleHookFinalizesWhenMetadataMissing|TestOpcodePlanCompatibilityAtResponseHandler|TestHandleGeneratorResponse_CoalesceKeyBySDKRequestVersion|TestScheduleQueueShardNotFoundCleansFreshState|TestDeferFinalize|TestDeferAdd|TestDeferAbort|TestFinalizePublishesFnFinishedOnce|TestFinalizeRetriesFnFinishedAfterPublishFailure|TestFinalizeDeleteFailureDoesNotLoseFinishEffects|TestParallelPauseBackedOpsCoalesceDiscovery|TestSleepCompletionSaveRetries)$'
```

The exact test sources are [DB adapter integration tests](../../../../../pkg/db/adapter_integration_test.go), [DB regression tests](../../../../../pkg/db/regression_test.go), [CQRS CEL tests](../../../../../pkg/cqrs/manager/cqrs_test.go), [queue Lua compatibility tests](../../../../../tests/execution/queue/lua_compatibility_test.go), [state-store Lua compatibility tests](../../../../../tests/execution/state_store/lua_compatibility_test.go), [executor cancellation test](../../../../../pkg/execution/executor/cancel_test.go), [opcode compatibility test](../../../../../pkg/execution/executor/compatibility_golden_test.go), [response coalescing test](../../../../../pkg/execution/executor/discovery_coalesce_test.go), [queue-shard routing test](../../../../../pkg/execution/executor/schedule_queue_shard_test.go), [defer lifecycle tests](../../../../../tests/execution/executor/defer_test.go), [finalization tests](../../../../../tests/execution/executor/finalize_duplicate_test.go), [parallel coalescing tests](../../../../../tests/execution/executor/parallel_invoke_coalesce_test.go), and [sleep retry tests](../../../../../tests/execution/executor/sleep_retry_test.go).

## Recorded results

Counts below are top-level Go test functions reported by verbose output; nested
subtests are included in their parent and are not counted separately. Durations
are the package durations from Go's `ok` lines, not shell wall-clock times.
Every paired `.exit` file contained `0`.

| Run | Selection | Top-level result | Package duration |
| --- | --- | ---: | --- |
| Requested `001-db-sqlite` | `./pkg/db/...`, SQLite | 43 passed, 1 skipped | `pkg/db` 0.433s; `pkg/db/postgres` 0.032s; `pkg/db/postgres/sqltypes` 0.003s; `pkg/db/sqlite` 0.023s |
| Requested `003-db-postgres-exact` | `./pkg/db`, PostgreSQL | 35 passed, 1 skipped | `pkg/db` 57.889s |
| Requested `004-cqrs-sqlite` | Two selected CEL manager tests, SQLite | 2 passed | 0.238s |
| Requested `005-cqrs-postgres` | Same CEL manager tests, PostgreSQL | 2 passed | 3.931s |
| Requested `006-queue-lua-sqlite` | `TestLuaCompatibility`, Valkey | 1 passed | 18.529s |
| Requested `007-state-store-sqlite` | Five selected state-store tests, Valkey/SQLite fixtures | 5 passed | 24.240s |
| Requested `009-executor-sqlite` | Twelve selected executor tests | 12 passed | `pkg/execution/executor` 0.040s; `tests/execution/executor` 2.538s |
| Extra `002-db-postgres` | `./pkg/db/...`, PostgreSQL | 43 passed, 1 skipped | `pkg/db` 62.820s; `pkg/db/postgres` 0.019s; `pkg/db/postgres/sqltypes` 0.003s; `pkg/db/sqlite` 0.034s |
| Extra `008-state-store-postgres` | Same five state-store tests, Valkey and selected DB fixtures | 5 passed | 23.984s |
| Extra `010-executor-postgres` | Same twelve executor tests | 12 passed | `pkg/execution/executor` 0.038s; `tests/execution/executor` 2.576s |
| Extra `011-queue-lua-postgres` | `TestLuaCompatibility`, Valkey | 1 passed | 14.055s |

Across the eleven invocations, 161 top-level test functions passed and there
were three skip events, all from the same DB test in the three DB selections.
There were no failures or nonzero exits.

## Skip and interpretation

The sole skipped test was
[`TestDeleteFunctionsByIDsHandlesDuplicateRows`](../../../../../pkg/db/regression_test.go#L417).
Its setup inserts duplicate function IDs and skips when the schema's uniqueness
constraint rejects the second row, making that legacy duplicate-row scenario
unreachable on this schema. Go reported the skip at 0.00s in `001-db-sqlite`,
2.89s in `002-db-postgres`, and 1.80s in `003-db-postgres-exact`. These are the
same structural skip across three invocations, not test failures.

The requested and extra queue/state PostgreSQL-environment runs are useful
environment checks, but the Valkey Lua tests still exercise Valkey. Similarly,
the executor integrations that use the explicit SQLite helper do not add
PostgreSQL executor coverage. The positive PostgreSQL evidence here is the real
`pkg/db` test-container run and the two selected CQRS CEL manager tests run
against PostgreSQL. This is a bounded service subgate only: it is not the full
portable SQL server corpus, does not run a Rust implementation, and supplies no
Linux/arm64 or E2E result. Broad Phase 1 and the later platform, parity, and
rollout gates remain open.
