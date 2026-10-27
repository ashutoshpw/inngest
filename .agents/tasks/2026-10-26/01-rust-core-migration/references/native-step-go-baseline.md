# Native-step Go persistence baseline

Parent-accepted bounded owner baseline after independent Sol queue and Luna state reviews found no blockers. Base: `903288e1b18fcb8dcb99e9d64500ff0ba4b48256` (`t3/fixed-screen-mode`, PR #14). The delivery branch is `t3/native-step-persistence-baseline` in `/home/ashutosh/.t3/worktrees/inngest/t3-01bab1c9`.

## Accepted amendment and bounds

Parent approved stopping FFI expansion after accepted SCREEN_NO_GO. Preserve the original evidence. Establish Go accepted response → checkpoint → discovery first; fresh-runtime replay is a separate next layer. One ordinary non-AI StepRun, fixed identities/time/request version 2, empty SDK metadata and experiments, ForceStepPlan false. Both queue modes, logical AlwaysShardOnRun/NeverShardOnRun, default/custom prefixes. No custom concurrency, semaphore, singleton, throttle, queue override, capacity lease, Constraint API, hints, promotion, resolver or enqueue hook. Held pending entries are inert preconditions, not parallel work.

No native adapter/client dependency selection, worker, benchmark, Rust edits, activation, active lease/fencing/crash/production claims. Go replay and native dependency qualification are required before porting. Rust 1.94 and existing serde/json/xxhash remain unchanged. Reconcile repository SSPL/workspace Apache licensing before distribution.

## Accepted P3 assertion limits

- The queue-script wrapper accepts either the manifest SHA through `EVALSHA` or the exact expanded script through `EVAL`. Assertions pin the source/include/expanded-script hashes and expanded script semantics, but do not require `EVALSHA` as the only selected command path. The separate SaveStep cache-flush case exercises the real `EVALSHA` → `NOSCRIPT` → exact-script `EVAL` fallback.
- For the response-generated discovery item, the test checks JSON structure plus generated trace/group bindings and retains/compares the actual stored item bytes as observed store state; it does not pin an exact whole-response queue-item encoding. The standalone queue-owner fixture still pins its item bytes literally.
- PreparedStepRun output bytes are literal-byte-pinned. A future native adapter must preserve the accepted preparation, persistence, retry, dedupe, indexing, and tombstone semantics within these boundaries. This baseline does not claim that every requested wire byte or script-command selection is frozen.

## Test authoring gate

1. Protect independently specified output bytes, accounting and error/pending identity at SaveStep, real store state after execution, and persisted discovery/dedupe semantics.
2. Credible regressions: JSON number/order loss, repeated accounting after a committed reply is suppressed, ignoring authoritative pending, wrong queue score/index/identity, or callbacks after dedupe.
3. Existing forty continuation contracts own sequencing; six native Valkey cases own their tagged host boundary. Existing queue/run-state fixtures own general serialization. New cases cover Go-only real-store behavior and adapter retries that those owners cannot reach; reuse setup and existing overflow/metadata-delta assertions.
4. No production seam. Test-private clients forward real commands and use independent cache-disabled connections for store observations. Suppressed postcommit responses are not network partitions. Never fabricate successful Lua results or seed expected effects.

## Checklist

- [x] Verify clean worktree, ancestry and exact local base; read owners and existing references.
- [x] Record parent-approved amendment without closing broader gates.
- [x] Portable literal preparation and persistence contracts, raw script provenance.
- [x] Go real-entry persistence/discovery witness with real tracing.
- [x] SaveStep pending, conflict, input accounting and suppressed-reply retry table.
- [x] Queue indexes, producer dedupe and bounded genuine tombstone TTL witness.
- [x] Focused normal/race validation with no required skips.
- [x] Independent Sol queue and Luna state reviews accepted with no blockers; parent accepted the bounded baseline.

## Evidence

Direct Docker invocation failed with socket permission denied; previously authorized `sudo -n -u ashutosh -g docker` succeeds. Existing helper uses authenticated AOF Valkey 8.0.3 linux/amd64. Resolved image ID `sha256:88251207ac13df6d4bd4d3a77e6953ca3b90e0e5cc6acb6b3ece0fc2f9091289`; repo digest `valkey/valkey@sha256:4e78d8ccab4d8978afca140a3dd75b4e45d683d1e51602ee71e976bcc2bd939e`.


## Test ownership and portable contracts

- `pkg/execution/executor/response_native_baseline_test.go`: untagged `HandleGeneratorResponse` through `responseRunner.step`, `prepareStep`, real tracing, real v2 SaveStep and queue producer. Seven literal outputs × both queue modes × both logical state layouts × default/custom prefixes = 56 real-store cases. Create the run and actual initial ordinary edge first; fixed event identity, UTC time, request version 2, no AI/SDK/experiment metadata. The lifecycle/release observer for the exact accounting threshold is reused from the existing owner tests.
- `response_storage_helpers_test.go`: only test-private constructors/observers/fixture decoding. Constructors moved out of the tagged native storage test; its `AlwaysShardOnRun` default and queue construction are preserved. No native driver or recovery harness is copied into the baseline.
- `pkg/execution/state/redis_state/native_step_adapter_test.go`: 84 cases (two layouts × two prefixes × three pending preconditions × seven save scenarios). Existing `v2_adapter_test.go` remains the general adapter owner. Test-private script forwarding and independent whole-store reads add the previously missing actual retry/commit evidence.
- `queue_native_persistence_compat_test.go`: 20 cases (two queue modes × two prefixes × fresh/identical/conflict/committed-reply-suppressed/run-ID-fallback). Existing queue-item and run-state fixtures remain the general serialization owners.
- Portable `executor/testdata/compatibility/native-step-preparation-v1.json`: exact prepared/saved strings and UTF-8 byte counts for missing/null, `9007199254740993`, `1e+06`, `-0`, whitespace-compacted b-before-a object, and a string with quote/backslash/newline/HTML/Unicode U+2028/U+2029. Expected values are checked in; tests do not regenerate them.
- Portable `state/redis_state/testdata/compatibility/native-step-effects-v1.json`: initial run metadata/event bytes, 21 literal adapter cases (sentinel/error class, pending, attempts, actual Lua results, remaining pending and accounting), ordered five save keys and twenty-two enqueue keys (including absent placeholders), hash known answers, literal standalone queue-owner item/partition/backlog/shadow records, whole queue-store snapshots, timing/score units and the six-phase observation sequence. The response-generated item has structural plus trace/group-binding assertions; its observed store bytes are compared as actual state, not against an exact whole-response queue-item encoding. Prefix/run-tag substitution does not change tenant IDs, stable identities, counters or scores.

All independent observers use the existing cache-disabled helper to open a separate Valkey connection. They SCAN the complete isolated store, sort keys, and read TYPE/GET/HGETALL/LRANGE/SMEMBERS/ZRANGE WITHSCORES/PTTL. Whole-map comparison rejects unexpected keys as well as missing keys. Raw observations are logged before comparison; no intercepted KEYS/ARGV supplies store truth. Persistent keys require PTTL -1. Absent enqueue tombstones require -2. The run-creation idempotency key has a one-day TTL: only its PTTL may decrease by at most five real seconds between observations; raw values remain in the logs.

The real-entry phases are precreated, pre-save, post-save, pre-discovery, post-enqueue and fresh-adapter-duplicate. Checkpoint output/accounting must already be visible before discovery starts. Seven output bytes are checked both at SaveStep admission and through a fresh store reader. Exact overflow accounting is 34 loaded bytes + 13 output bytes: limit 47 accepts; limit 46 rejects before save/lifecycle/release/enqueue with the original overflow sentinel inside the direct standard wrapper. NoRetry/RetryAt identity behavior remains owned by the existing overflow test. Ordinary eligible metadata contributes zero; positive WithMetadataSizeDelta is supplied separately as legitimate adapter input. Neither positive accounting nor these spans is claimed to prove generation of AI/SDK metadata.

## SaveStep case evidence

Each scenario runs against pending `[]`, `[s]`, `[s, held]`; held is an inert stored precondition. SaveResponse removes only s. An identical new request reports the *current* pending set with the original `ErrIdempotentResponse` sentinel and no accounting/stack increment. A conflicting request returns the original `ErrDuplicateResponse` sentinel and false, retaining original bytes. The success response owner accepts both sentinels, reaches real queue dedupe, and suppresses scheduled callbacks.

- Fresh save: one real write, state size +25, step_count +1 and one stack entry; current pending accurately returned.
- Identical separate request: reset pending through the real SavePending owner to `[s, held]`; result is `[-2, 1]`, original sentinel, no write/removal/increment.
- Conflict: result `[-1]`, original duplicate sentinel/false; original output and current pending remain.
- Committed response suppressed once: require actual success `[0]` or `[1]`, independently observe the committed store, then suppress only its reply. A separate canceled-context command produces the real error result while the caller context remains live. Actual adapter attempt 2 returns `[-2, currentPending]` and is swallowed to nil; only one write/accounting/stack increment.
- Retries disabled: the same genuine commit/suppression reaches the actual `util.ErrMaxAttemptReached` exhaustion class, not context cancellation identity (the existing exhaustion wrapper formats its last cause). A freshly constructed adapter observes the committed output and returns original ErrIdempotentResponse/current pending.
- Transient preexecution failure with a preexisting conflicting output: first attempt executes no save; actual attempt 2 returns duplicate. The current adapter heuristic swallows it to false/nil. This is explicitly **not proof that attempt 1 committed**.
- Step inputs + positive metadata delta: initial state_size 61 includes the serialized input envelope; saved input bytes are 7, output bytes 25, so save adds 18, step_count/stack once and metadata_size +11 once. The stored input remains present; identical re-save adds nothing.

## Queue case evidence

Final logical ID is functionUUID:configured-idempotency-stepID, or functionUUID:runULID-stepID when configuration is empty. Known answers are unsigned XXH64 seed 0, lower base36; captured independently at authoring with system libxxhash, not the Go HashID implementation. Configured answer `dwublzzf4wu6`, fallback `hxmpw17953kl`; include empty/a/hello controls. The Go response owner selects the logical ID; the actual queue producer hashes it and fresh LoadQueueItem restores Data.JobID from the hashed outer ID.

Pin generation 1, zero attempts/max 3, fixed tenant/function/run/event identifiers, compact ordinary edge payload, omitted optional fields, enqueued time and wall time `1793016000000`, priority factor 7 seconds, AtMS `1793015993000`, and ULID-derived queue score `1469922843259` milliseconds (ULID time `1469922850259` minus 7000). Legacy global partition/account pointers use seconds `1793016000`; key-queue shadow/backlog pointers retain millisecond queue scores. Both modes include the item hash, partition metadata, run/status indexes. Legacy additionally has ready/global-partition/account-partition/global-account indexes; key queues have backlog item/meta, shadow meta/backlog pointer/global-shadow/account-shadow/global-account-shadow indexes. All unused placeholders, seen, singleton and normalization keys remain absent.

Identical and conflicting producers return the typed queue duplicate sentinel with the original owning run and leave *all* raw bytes/indexes unchanged. For a committed reply suppressed once, the producer reports the real cancellation result; a new producer sees the actual retained item and duplicates. This is test-private postcommit response suppression, not a network partition or general uncertain-ack recovery proof.

Four fresh cases genuinely Dequeue the persisted item using a one-second item idempotency period. Seen becomes an empty string with positive PTTL <=1000 ms; a new duplicate has nil RunID because no live item remains. A bounded three-second poll of real Valkey expiry precedes successful re-enqueue. Advancing the fake queue clock is never used to expire Redis keys.

The real OTel provider prepares step spans before SaveStep and materializes the discovery carrier in persisted metadata. Validate UUID v4, enqueue-context/group agreement, trace formats and exporter/carrier relationships before using generated values in literal JSON comparison. The deterministic root trace ID and parent are fixed literals; metadata must contain exactly one propagation carrier with exactly tp/ts/dstp/dsid. Unknown metadata/carrier fields cannot be normalized away. Keep all raw values. Tenant identities are a persistence contract, not an authorization claim. The generic discovery port failure is AlwaysRetry with no scheduled callback; the existing outer AlwaysRetry wrapper has no Unwrap, so that case asserts its observable type/policy/message rather than inventing an error-chain identity guarantee. SaveStep and overflow retain exact sentinel identity assertions.

## Lua provenance

Raw source paths: `pkg/execution/state/redis_state/lua/saveResponse.lua`, `lua/queue/enqueue.lua`, and the latter's direct `lua/includes/*.lua` references listed in the fixture. The test independently reads the files and performs the existing one-pass direct include expansion in Go, comparing raw/include SHA-256, expanded byte count, expanded SHA-1 and SHA-256 to the literal manifest. No TestLuaScriptSnapshots invocation and no expected-file writes. The forwarding wrapper accepts either the manifest EVALSHA SHA or the exact EVAL body and compares ordered KEYS/ARGV; it does not impose an EVALSHA-only command selection. A deliberate script-cache flush exercises the real EVALSHA/NOSCRIPT → EVAL fallback for saveResponse. Effective attempts and actual Lua results are observed separately from the NOSCRIPT lookup.

## Validation and retained failures

Go binary `/home/ashutosh/.cache/go1.26.4/go/bin/go` (Go 1.26.4 linux/amd64). Every command uses `sudo -n -u ashutosh -g docker env GOTOOLCHAIN=local GOMAXPROCS=2 GOTMPDIR=/home/ashutosh/.cache/native-step-baseline/tmp TMPDIR=/home/ashutosh/.cache/native-step-baseline/tmp`. The default /tmp volume was nearly full, so both temporary directories are explicitly on the cache volume.

Normal selection (same selection for -race):

```sh
go test -json -p=2 ./pkg/execution/executor ./pkg/execution/state/redis_state \
  -run '^TestNativeStep|^TestResponseContinuationContract$|^TestStagedBatch|^TestQueueItemPersistenceCompatibility$|^TestRunStatePersistenceCompatibility$' \
  -count=1 -timeout=180s
```

First final normal pass: `/home/ashutosh/.cache/native-step-baseline/final-normal-02.jsonl`; executor 173 case passes / 3.899s, redis_state 111 / 11.142s, zero failures/skips. Race uses -race and timeout 240s. Final-source evidence is recorded below. These counts include parent tests and subtests, not separate top-level functions. The old six Rust-tagged native Valkey cases were read as primary prior evidence and their constructors reused; they were not rerun here and the Go baseline needs no Rust library or tag.

Retain earlier failed local logs in `/home/ashutosh/.cache/native-step-baseline/`: adapter-normal.log (moving run-idempotency PTTL); executor-normal.log (missing vs nonnil empty RawMessage, priority units, threshold helper's fixed message); executor-normal-02.log (unused variable compile repair); executor-normal-03.log (incorrect zero dynamic root-span expectation); queue-normal.log (mistyped global normalization placeholder); combined-normal.log (queue identifier intentionally omits run idempotency key in V1FromMetadata); combined-normal-02.log (dropped discovery spans are filtered before exporter); owners-normal.log / owners-normal-02.log / final-normal.jsonl (incorrect error unwrap/wrapper-identity assumptions). All are harness/expectation repairs checked against the actual owners, with no production edits or generated-at-test expected updates. Source hashes/includes match; no unexplained store key is accepted.

`scripts/check-plan-status.sh` still fails on the pre-existing invalid `docs/plans/run-ai-debugging.md` status `Implemented — pending staging review`; see local plan-status.log. This file is unchanged. `git diff --check` passes.

## Delivery metadata

PR [#15](https://github.com/ashutoshpw/inngest/pull/15), `test(state): freeze native step persistence interoperability`, targets `t3/fixed-screen-mode` (PR #14) from `t3/native-step-persistence-baseline`. The opening implementation commit `4e1cfa249680232a1d2fa3a373ee6f065ce2cf43` is based on `903288e1b18fcb8dcb99e9d64500ff0ba4b48256`. The PR is linked to the delivery child and parent migration thread. Opening snapshot: open, non-draft, merge state clean, no review decision, no check runs. A follow-up commit records this metadata; the next fresh-runtime replay layer will base on the final PR #15 commit.

## Remaining gates

Fresh Go runtime replay is the next layer. SDK compatibility policy, broader recovery, process/lease/fencing/crash durability, native dependency qualification, arm64 execution, bridge choice, license reconciliation, and production activation remain open. All earlier accepted negative screen evidence remains intact. No benchmarks, Rust source/dependency changes, worker or production seam were added.


### Final-source delivery evidence

After moving all 21 adapter outcome rows into the fixture and tightening exact carrier schema/fixed root IDs, `accepted-normal.jsonl` passes executor 173 cases / 3.911s and redis_state 111 / 10.680s, zero skips. The final additional group/context agreement assertion then passes the focused `^TestNativeStep` executor normal selection in `delivery-normal.jsonl`: 63 cases / 3.981s, zero skips. No Redis owner/fixture changed after accepted-normal. The final full selection with `-race` passes executor 173 / 6.151s and redis_state 111 / 12.537s in `delivery-race.jsonl`, zero failures/skips. Older `final-race.jsonl` also passed before the final fixture/trace refinements and is retained as historical evidence, not substituted for the final run.

All logs are under `/home/ashutosh/.cache/native-step-baseline/`:

- `accepted-normal.jsonl` SHA-256 `945d3ba455232ab7394d96966b257c2d6f6756a0d9e976827f0c0266df023f4a`.
- `delivery-normal.jsonl` SHA-256 `617d8e072e83bc60c1ff0bdd841fc284ce9dfeffebc8eb27245d18fba42b90e2`.
- `delivery-race.jsonl` SHA-256 `94339114766e23d14b2d0b314f91ffdd277fafc8797b1dab876a1a3b92c88ee1`.
- `evidence-sha256.txt` retains hashes of prior passing and failed logs, including original failures. These local logs are not committed portable artifacts; the two literal JSON contracts and source test assertions are reviewable/reproducible repository artifacts.

Preparation fixture SHA-256 `391d5487e36815803a6da76b01e2fa66b32620e351878e895496ed8eac275c14`; persistence fixture SHA-256 `106e5c1f31fa30e9a608f495f8e3db9fdc2595be0299bd41dd9d5cc11fa9fe94`.
Expanded saveResponse: 1611 bytes, SHA-1 `2c371556f31bb03e150253bbd588a9a34255061a`, SHA-256 `402c8d9e0a6de6de2d6117c0b69f5a6e4823410c05437dc46c30a7aa0d3b9409`.
Expanded queue/enqueue: 23635 bytes, SHA-1 `3868a424cbc217eed464dfef3f1618d64794c103`, SHA-256 `3e4d40b2c83b4a6e51127c1888a5cd3d452bd41afee9caca1d330af86c3837e0`.

Final gofmt and whitespace checks pass. Only tests/fixtures/task documents differ from base; production Go, Lua, Rust, dependencies and original screen capture artifacts are unchanged. Git worktree ownership confirms branch `t3/native-step-persistence-baseline` at base `903288e1b18fcb8dcb99e9d64500ff0ba4b48256`, checked out at `/home/ashutosh/.t3/worktrees/inngest/t3-01bab1c9`.
