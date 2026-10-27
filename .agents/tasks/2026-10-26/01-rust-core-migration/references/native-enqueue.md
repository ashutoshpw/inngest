# Bounded native ordinary StepRun discovery enqueue

State: accepted after independent Sol review 03, Luna review 02, and parent
acceptance. [PR #18](https://github.com/ashutoshpw/inngest/pull/18) is open,
stacked on PR #17. Internal only; licensing unresolved.
Phase 1 partial, Phase 2 open, bridge unset.

Worktree `/home/ashutosh/.t3/worktrees/inngest/t3-45e16f5c`, T3-bound branch
`t3/native-step-enqueue`. Verified clean and no unique commits, then fast-forwarded
in place from `a4592cde49cab4c4a1afc5488cf39345514d5ed7` to accepted PR17 base
`928af60e182e7dd7b679c5ac819b1fa74107cc82`. Sibling accepted checkout is read-only.

## Scope and authoring gate

Rust owns logical discovery ID, unsigned XXH64 seed 0 base36 ID, ordinary item,
22 KEYS / 14 ARGV, existing expanded Lua and native wire/result contract. Host
supplies identity, response, authoritative save completion/pending, original
incoming edge, opaque group/carrier, explicit clock and bounded queue settings.
No prepared enqueue recipe, host enqueue, trace generation or lifecycle claim.
One endpoint; separate state/queue prefixes. No automatic enqueue retries.

Tests protect distinct independent boundaries: valid-prefix transport overflow;
unchanged literal args/store fixture via native sockets; actual preexecution and
independently observed lost-ACK commits; native save/enqueue followed by fresh Go
reads and normal fresh scanner/HTTP replay. Existing Go tests cannot reach Rust
transport behavior. Reuse fixtures/readers/recovery owners; no new production seam.
Each test must fail for a credible contract regression, not source refactoring.

## Checklist

- [x] Binding/base/owners/fixtures/skills read; full script and fixture hashes verified.
- [x] Helper cap+1 rejection and demonstrated before/after boundary regression.
- [x] Strict bounded descriptor, native item/keys/args, single-attempt runtime.
- [x] Fixture/socket/duplicate/no-socket interop and fresh-owner replay.
- [x] Focused normal/race, Rust fmt/clippy/tests/helper and four target checks.
- [x] Evidence, limits and uncommitted independent-review handoff.
- [ ] Independent Luna + Sol review and parent acceptance of this implementation.

## Manifest provenance

Unchanged persistence fixture SHA256
`106e5c1f31fa30e9a608f495f8e3db9fdc2595be0299bd41dd9d5cc11fa9fe94`;
preparation fixture SHA256
`391d5487e36815803a6da76b01e2fa66b32620e351878e895496ed8eac275c14`.
Queue raw SHA256 `4a0576efc4302310714fbafc40e43949e364c10ecc452be9b9efdfc2ddaeb261`;
expanded 23635 bytes, SHA1 `3868a424cbc217eed464dfef3f1618d64794c103`,
SHA256 `3e4d40b2c83b4a6e51127c1888a5cd3d452bd41afee9caca1d330af86c3837e0`.
Six unique includes (seven include directives) verified against the unchanged
fixture; full values retained in `/home/ashutosh/.cache/native-step-enqueue/manifest.log`.
No Lua copies or fixture regeneration. Drift is a stop condition.

Native command contract is explicitly EVALSHA -> NOSCRIPT -> exact EVAL, no
SCRIPT LOAD. The Go queue baseline did not pin command selection. Save and
enqueue remain separate commits; lost ACK yields uncertainty, never certainty
or automatic enqueue retry. Drained-owner replay does not prove crash recovery,
fencing, atomicity, active-lease recovery or general exactly-once semantics.
At the accepted PR #18 implementation commit, preparation still compacted the
full value before checking the 4 MiB output limit. The later [bounded preparation
memory increment](bounded-preparation-memory.md) supersedes that compaction claim
for SaveStep preparation: retained data and prepared output now have independent
4 MiB bounds. Input, parser and whole-process memory remain outside the guarantee;
enqueue serialization keeps its existing unrestricted compaction API.

## Validation history

Local logs under `/home/ashutosh/.cache/native-step-enqueue/`; preserve failures.
Initial helper regression build omitted TMPDIR and failed linking on full /tmp;
retry uses the required cache temp directory. No test assertion ran in that build.

## Implementation and admission

Production owners inspected: executor `response_queue_effects.go` discovery
binding/admission/error mapping; queue `enqueue.go`, `item.go`, partition/backlog/
shadow/key generators and Redis queue `EnqueueItem`; accepted SaveStep preparation,
state adapter and source Lua; the existing real-store recovery owner. No production
Go owner or Lua source changed. Native code remains unpublished and unconnected.

`enqueue.rs` accepts a private prepared StepRun and typed source inputs, producing
all queue bytes/keys/arguments internally. Unknown fields fail descriptor decoding.
Save must already have completed and its authoritative pending result must be false.
Nonempty ordinary SDK `name` strings are now admitted: name does not affect saved
output or discovery identity; Go still owns lifecycle use of that field. There is
no parser rewrite. Force-plan, failure/planned opcodes, multiple ops, nonempty opts,
AI/metadata and all excluded descriptor extensions reject before opening a socket.
Queue/function overrides, concurrency/semaphore/throttle/singleton, coalescing,
normalization/promotion, capacity leases, custom routing, cluster and physical
sharding remain excluded. The simple identity excludes batch/replay/original-run
extensions. Exactly one canonical event ID is admitted and must match eventID; explicit
batch descriptor fields (including null) reject during strict input decoding;
clock must be positive and no earlier than the run; zero AtMS and scores/clocks
outside Lua's exact integer domain reject. These are bounded admission limits.

Logical ID is `function:(idempotency or run)-step`, stored ID is unsigned
XXH64(seed=0) lower base36. Generation starts at 1, attempt at 0. Item AtMS is
explicit now minus priority seconds converted to ms; attempt-0 score is run ULID
timestamp minus that priority (zero timestamp falls back to now). Partition
pointer uses max(score, now) in seconds; backlog/shadow scores use ms. Native
serialization follows the existing Go field order/omissions and HTML escaping.
Key shadow `fv` carries the identifier function version. Backlog `fv` carries
its earliest function version and is omitted only at zero; the partition-mode
shadow placeholder remains `{"fv":0}`. This corrects the initial version-zero
assumption, as verified by the resumed Go-owner differential below. All 22 keys and 14 arguments are native-derived. State layout/prefix
validation is separate from the single queue prefix and endpoint.

Runtime enqueue is separate from the SaveStep retry loop. Acknowledged code 0
means Enqueued/Confirmed; 1 performs a single HGET for the current item's RunID,
or returns Duplicate with no RunID for a tombstone. A bad lookup is Failed, not
Duplicate success. Code 2 is ExcludedSingletonConflict (an error, outside admitted
singleton scope). Possibly sent transport/protocol failures return Failed and
MayHaveCommitted with no invented scalar result. No reconnect/retry, Go enqueue
fallback or automatic uncertainty reconciliation occurs. The test interop adapter
constructs Go QueueItemExists only for a native Duplicate; unchanged Go discovery
treats that sentinel-compatible wrapper as success and every other error as
AlwaysRetry. Native classes themselves have no Go sentinel identity.

The feature-gated one-request helper adds enqueue, cap+1 read/rejection, and an
optional explicit Go-owned metadata delta for prepareSave. This allows HTTP replay
to prepare original response bytes in Rust while retaining observed Go HTTP timing
metadata. No fixture supplies native prepared output or an enqueue recipe.

## Independent witnesses

`TestNativeRustEnqueueInterop` reuses nativeScript/nativeQueueKeys/nativeQueueArgs/
expectedNativeQueueSnapshot and the unchanged item literal and IDs. Sixteen
mode/layout/state-prefix/queue-prefix combinations each cover cold-cache success,
preexecution socket drop, commit/reply loss, and run-ID fallback. Every run first
performs native preparation/SaveStep, then native enqueue, then fresh cache-disabled
Go state/queue reads and complete raw-store comparison. Cold cache checks exact
EVALSHA/EVAL wire bytes; the fault relay independently observes a real commit while
the native caller is alive before dropping its genuine ACK. Only the harness knows
that commit happened; the native result remains uncertain. Exact command counts
prove no hidden retry, including with the helper's SaveStep retries enabled.

Live duplicate checks use a different candidate run sharing the logical ID and
require the original current item's RunID. Real Go dequeue creates the tombstone;
native duplicate then returns no RunID and preserves stored values/indexes, with
independently bounded natural TTL decay. Eighteen unsupported cases assert zero
native sockets, including handshakes. The seven preparation answers and existing
SaveStep/storage/runtime tests remain independently selected.

`TestRecoveryReplaysNativeEnqueue` reuses the existing three-scenario recovery body
through test-private effect substitution. A uses native preparation/save, then
native queue serialization/writes; Go owns opaque group/carrier generation.
Actual TCP drops replace the Go baseline's injected precommit/ACK errors. Native
ACK loss is independently read before socket closure; the native outcome still
has no result and MayHaveCommitted. Matching subsequent native work stays native,
without falling back to Go. A drains; B opens fresh ordinary Go owners, loads the
existing SQLite registration without upsert and normally scans durable jobs. No
captured/manual job injection. Saved bytes/stack/metrics, raw index scores,
dedupe, HTTP identity/checkpoint and non-deduplicated external effect count 1 all
pass. Response-generated whole items remain structural with carrier/group binding
checks; no new whole-item byte oracle was invented.

The unchanged Go-only replay and original Sleep recovery run alongside native
replay in both queue modes. Twelve fixture/README/original recovery source files
were byte-compared to base; their hashes are in preserved-baseline-sha256.txt.
Only stores and HTTP endpoint survive owner replacement. No process crash, active
lease recovery, fencing, scheduler expansion, new schema, atomic save+enqueue,
store restart/fsync/disk-loss or general exactly-once guarantee is claimed.

## Final validation

Go `go1.26.4 linux/amd64`, Rust `1.94.0`. Actual authenticated AOF Valkey
`valkey/valkey:8.0.3`, image ID
`sha256:88251207ac13df6d4bd4d3a77e6953ca3b90e0e5cc6acb6b3ece0fc2f9091289`,
repo digest `valkey/valkey@sha256:4e78d8ccab4d8978afca140a3dd75b4e45d683d1e51602ee71e976bcc2bd939e`,
amd64. Existing helper retains requirepass and appendonly yes, with fresh owned
containers only; SQLite survives A/B in the same temporary directory.

All final Go commands use this exact prefix (KEYS is false/true below):

```sh
sudo -n -u ashutosh -g docker env \
  GOTOOLCHAIN=local GOMAXPROCS=2 \
  GOTMPDIR=/home/ashutosh/.cache/native-step-enqueue/tmp \
  TMPDIR=/home/ashutosh/.cache/native-step-enqueue/tmp \
  EXPERIMENTAL_KEY_QUEUES_ENABLE=KEYS \
  NATIVE_STEP_TEST_BINARY=/home/ashutosh/.cache/native-step-enqueue/native-step-helper-rust194 \
  /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -p=2 -tags=native_step_integration
```

Two owner invocations use KEYS=false, once normal and once with `-race`, followed
by `./pkg/execution/state/redis_state ./pkg/execution/executor -run
'^TestNative(Rust|Step)' -count=1 -timeout=180s`. Four replay invocations use each
KEYS value, normal and `-race`, followed by `./tests/execution/executor -run
'^TestRecoveryReplays(NativeEnqueue|NativeStepBaseline|PersistedSDKResponse)$'
-count=1 -timeout=180s`. Literal argv are retained per log in `.command.json`;
the cache validation script runs at most two commands concurrently.

Final six commands all exit 0: owners normal 346 Redis-state / 63 executor pass
records (64.930 / 3.366s); owners race same counts (72.033 / 5.598s). Replay false
normal/race 9 each (13.511 / 16.723s); true normal/race 9 each (14.849 / 16.285s).
Total 854 test/subtest passes, zero failures, zero skips, no race report. These are
focused selections, not full suites; no benchmark ran. Five Rust tests pass (four
existing preparation tests plus helper boundary regression), zero ignored/failed.

From `rust/`, using `TMPDIR=/home/ashutosh/.cache/native-step-enqueue/tmp` and
`CARGO_BUILD_JOBS=2` for build/check commands:

```sh
cargo +1.94.0 test --locked -p native-step-state --all-targets --features test-helper
cargo +1.94.0 build --locked -p native-step-state --features test-helper --bin native-step-test-helper
cargo +1.94.0 clippy --locked -p native-step-state --all-targets --features test-helper -- -D warnings
cargo +1.94.0 fmt --all --check
cargo +1.94.0 check --locked -p native-step-state --all-targets --features test-helper --target TARGET
```

Each TARGET passed separately: x86_64-unknown-linux-gnu,
x86_64-unknown-linux-musl, aarch64-unknown-linux-gnu, aarch64-unknown-linux-musl.
Only x86_64 GNU ran; the other targets are compile evidence only. The fixed final
helper SHA256 is `672e40006fe72bc4674502e919f252ba01689e8da80353faa8638ab6d2ffa303`.
Direct xxhash-rust=0.8.15 uses the already-locked crate; lock diff adds only the
dependency edge. Redis remains exactly 1.8.0/defaults disabled/script only.

Final Go log SHA256:

- final-owners-False.jsonl: `150dec892f4f9331401d9f1cde73db41a79667f8d00413ec05f7d9ea3a80c326`
- final-owners-True.jsonl: `215e773bcc68e086cca586e8df8a1e55d0c3f313721078d862ebc7bbfee75d0a`
- final-replay-False-False.jsonl: `a146c5c31129ea9ad5b1aae396ce7fa74b318d2ce3f311254795cd91dcbc92ae`
- final-replay-False-True.jsonl: `2c9c534f351c006172e6679439ac343fc6a07fbcb2af896e6c2f21a88825def7`
- final-replay-True-False.jsonl: `7bb9c573a28798f24b84d4836f471a867937f31d604aa66d86232b749de42fdf`
- final-replay-True-True.jsonl: `924e41301f5b63c9cca75aa872a52e3b1223fdeff250015d8825fc220b1a658c`

`final-validation-summary.json` retains per-command/package counts/times/argv;
`evidence-sha256.txt` includes passing and failed logs. Logs are local evidence,
not new portable oracle fixtures. The demonstrated pre-fix helper failure is
helper-boundary-before-02.log (cap+1 returned success). enqueue-initial.jsonl
failed only on exact tombstone TTL comparison; the repair accounts for bounded
real decay while preserving full values/indexes. replay-false-initial.jsonl
failed on the existing named StepRun admission, repaired as described above.
clippy.log identified the large helper enum variant, repaired by boxing the
descriptor. A root-directory build selected default Rust 1.99.1; its preliminary
owners-normal/replay-true-normal passes are superseded by all final commands
against the explicit Rust 1.94 artifact. Initial full-/tmp build failures and all
these earlier logs are retained. No source manifest drift or unexplained store/
replay difference occurred.

Whitespace and affected Go formatting checks pass. Plan status still exits 1
only on unchanged docs/plans/run-ai-debugging.md status `Implemented — pending
staging review`. No unrelated plan status was edited. No commit, push, PR,
delegation, benchmark or production activation. Final review remains pending;
broader recovery, lifecycle/trace ownership, platform execution, production input
memory limits, licensing/distribution, bridge selection and rollout remain open.

## Accepted Sol P2 repair (2026-10-27)

Accepted finding: the 11 unsupported-descriptor cases in
`TestNativeRustEnqueueRejectsBeforeSocket` accepted any process error with empty
stdout. The supplied pre-fix `sol-review-missing-helper.jsonl` proves all 11
passed with `NATIVE_STEP_TEST_BINARY=/does/not/exist/native-step-test-helper`;
SHA256 `36e84a5d6563e36cd34b80a25a42d2334d007d5383a8df928322a9b3ed2f9df2`.
Those historical passes did not establish descriptor rejection. The earlier
854-pass record and all previous failure logs/hashes above remain preserved;
they are not whole-implementation acceptance.

The repair is test-only. Before Valkey startup and the table, a successful
preparation request uses the existing `invokeNative` artifact convention
(required environment variable, absolute existing file, successful execution and
JSON response). A minimal known step/output confirms a working helper rather
than an executable that always rejects. Each unsupported descriptor now requires
`*exec.ExitError`, exit status 2, exactly `invalid helper input\n` on stderr, and
empty stdout. Existing zero-command and zero-connection assertions remain for
all 18 cases. This guards the real helper/process and network boundary against
missing/wrong artifacts or unrelated crashes; it introduces no production seam,
source-shape test, or duplicate admission-policy oracle. Native implementation,
helper artifact and fixtures are unchanged. Helper SHA256 remains
`672e40006fe72bc4674502e919f252ba01689e8da80353faa8638ab6d2ffa303`.

Bounded verification uses the exact Go 1.26.4/cache-temp/GOTOOLCHAIN=local/
GOMAXPROCS=2/-p=2 command prefix above, KEYS=false, selecting only
`^TestNativeRustEnqueueRejectsBeforeSocket$`, `-count=1 -timeout=60s`, once normally
and once with `-race`. Both pass all 18 cases (19 pass records including parent),
zero failures/skips and no race report. This includes the intended real 11
unsupported rejections. No broad 854-case rerun or benchmark was performed.

The fixed missing-helper run fails the existing artifact-file precondition with
zero subtests and before Valkey starts, contrasting with the preserved pre-fix
11-case false pass. Four executable negative controls are local evidence only:
wrong-exit, wrong-diagnostic and crash forward the successful preparation probe
to the real helper, then respectively return status 3 with the intended
diagnostic, status 2 with an unrelated diagnostic, or SIGKILL for the selected
`coalescing` case. They fail specifically at expected status 2 (actual 3), exact
diagnostic comparison, and expected status 2 (actual -1). The always-reject
control returns even the intended status/diagnostic for every request and fails
the successful preparation prerequisite before any subtest. These controls prove
the rejection assertion and artifact prerequisite independently; they do not
implement an admission oracle.

Local evidence in `/home/ashutosh/.cache/native-step-enqueue/`:
`p2-validate.py`, `p2-validation-summary.json`, `p2-control-*`, and each log's
`.command.json` preserve exact commands/results. Log SHA256 values:

- p2-missing-helper.jsonl (exit 1): `17f39836d094f547d0ac9994f004eec287cd3e7143dd25ec973db5daf1adc393`
- p2-normal.jsonl (exit 0): `fe6fed4207c8c060f7a570c0c61a940c8c900ce5e8af689cdaacc8403b953e1a`
- p2-race.jsonl (exit 0): `3dd9f1b38e79402fa3ef8db8273c9fbf595878fc7b22711b730121f72e3fa8e1`
- p2-wrong-exit.jsonl (exit 1): `743f5b3b23dcc2c934db0d5d2a2dae1ebf999c53331d3d9d535fb6f2f9a659dd`
- p2-wrong-diagnostic.jsonl (exit 1): `c92833ecbcba056b11810ff6c5fb03157eebcebe0cfad0c8ebb82884b9e21376`
- p2-crash.jsonl (exit 1): `9b5be686eb59919bc277f1302db48b54b235c6a76a6020192a080eb96c30a426`
- p2-always-reject.jsonl (exit 1): `afc157727d2af39af2255612b3bc678bfbaf858588f40de51a0bfd4aba8511a6`

Fresh independent review of this P2 repair and parent acceptance remain pending;
the other native review lane remains read-only. This repair does not claim whole
acceptance, finish a migration phase, or select/activate a bridge. Prior limits,
failures and hashes remain intact, including the unrelated plan-status failure
on unchanged `docs/plans/run-ai-debugging.md` status
`Implemented — pending staging review`. No delegation, commit, push or PR.

P2 formatting (`gofmt -l`) and `git diff --check` pass. Re-running
`scripts/check-plan-status.sh` exits 1 with only the same invalid status above;
output is preserved separately as `p2-plan-status.log`. Comparing the prior
`final-source-sha256.txt` shows only the enqueue test changed; native source and
other recorded source files retain their hashes. P2 changed 14 added/1 removed
test lines plus reference/status documentation; no production lines changed.


## Resumed four-finding repair (2026-10-27)

Resumed in place after model capacity failure, on `t3/native-step-enqueue` at
`928af60e182e7dd7b679c5ac819b1fa74107cc82`. Read AGENTS, audit-test-cases,
existing tracked/untracked diff, owners, this reference/status and cached logs
before editing. The partial implementation, tests and historical evidence were
present; no reset, recreation or assumption of an empty worker result. Before
copies of enqueue.rs/runtime.rs/the enqueue test and their resume diffs are in
`/home/ashutosh/.cache/native-step-enqueue/parity-fix-01/`. The original helper
artifact is preserved at its original path/hash; new builds use the subdirectory.

The accepted rejection-test repair is **closed**, per the supplied Sol review02
handoff: independent normal/race runs passed all 18 cases, and missing-helper,
always-reject, wrong-status, wrong-diagnostic and SIGKILL controls failed for their
intended reasons. The prior section records its earlier pending-review state,
not a reopened finding. Its positive helper probe still precedes subtests/Valkey;
descriptor rejection still requires exit 2, exactly `invalid helper input\n`,
empty stdout and zero commands/connections. The helper cap+1 repair is untouched
and its Rust regression passes again. This resume addresses only the four native
findings; whole-implementation independent review remains pending.

### Repairs and real-owner regression contracts

- **Version persistence/update:** test inputs set `functionVersion` to 7, then
  11, then 9. Resulting stored versions are shadow 7 -> 11 -> 11 and backlog
  7 -> 7 -> 7. Key shadow serialization uses the identifier
  version; backlog serialization adds `fv` with Go's zero omission and field order.
  `TestNativeRustEnqueueOwnerParity` first enqueues actual Go `queue.Item`s through
  the ordinary Go queue owner into real Valkey, then repeats with Rust after a
  database flush and compares complete snapshots at each step.
  Both queue modes and carrier-present/absent cases run. This covers persistence
  and the unchanged Lua update branch; no Rust-generated expectations or fixture
  edits. Exact raw item/store comparison remains the standalone contract.
- **Batch exclusion:** multiple event IDs return Ineligible before queue item/args
  construction or network. Explicit `batchId`, including null, remains rejected
  by strict descriptor decoding; the rejection table now proves both forms and
  multi-event zero-network admission. The replay projection already asserted
  BatchID, original/replay IDs, concurrency, semaphore, and other excluded fields;
  an explicit one-event assertion now guards projection before building discovery
  input. No batch identity is silently projected away, and no batch support added.
- **Absent carrier:** empty metadata is omitted, matching Go `omitempty`.
  The owner differential above observes real Go omission and compares native
  queue item bytes exactly in both modes. Carrier-present baseline bytes remain
  unchanged; response/replay items retain their structural contract.
- **Duplicate readback:** success now requires decoding the relevant queue
  envelope, Item/identifier fields and edge-family payload types, rather than
  extracting runID alone. Bad lookup stays Failed/NotApplied with result 1 and
  `duplicate-item-decode`; it never becomes Duplicate. `TestNativeRustEnqueueDuplicateDecode`
  installs literal modified stored items, calls actual Go LoadQueueItem, then
  native enqueue and compares classification/current RunID and unchanged store.
  Its 27 cases include edge payload `[]`, string/nested malformed edges, invalid
  scalar/UUID/lease/metadata/enum fields, plus legitimate missing/null/empty fields,
  unknown edge fields, arbitrary metadata values, alternate UUID form, enum case
  handling and Go's non-strict ULID decoding. Zero-value and unknown-field handling
  follows Go rather than applying ordinary enqueue admission to stored items.

These cases protect observable storage/admission/readback contracts at the real
owner/socket boundary. Existing coverage used version zero, a carrier and valid
live items, so could not detect these regressions. No production Go/Lua edits,
production seams, new fixture expectations, new retry or fallback were introduced.
The key/arg/NOSCRIPT/lost-ACK/MayHaveCommitted witnesses run unchanged.

### Validation and retained failures

All new evidence is under the `parity-fix-01/` cache subdirectory above. Final Go
commands retain exact argv in each `.command.json`, counts/timings/hashes in
`final-validation-summary.json`, and raw JSONL. They use Go 1.26.4,
GOTOOLCHAIN=local, GOMAXPROCS=2, -p=2, cache-local GOTMPDIR/TMPDIR, and the rebuilt
Rust 1.94 helper, via authorized `sudo -n -u ashutosh -g docker env`. The six
selections/KEYS/race combinations are exactly those in the earlier Final
validation section, with the helper/temp paths under `parity-fix-01/`.

All six exit 0: owners normal and race each pass **382 Redis-state + 63 executor**
records; each of the four normal/race replay invocations passes 9 records.
**926 test/subtest pass records, zero failures/skips, no race report.** This is
focused affected-owner/replay validation, not a full-suite or performance claim.

Rust `rust-validation.json` records exact commands/log hashes. All eight commands
exit 0: Rust 1.94 fmt, clippy with -D warnings, all five tests (including cap+1),
helper build and all-target checks for each of x86_64 GNU/musl and aarch64 GNU/musl.
Build/check environment uses TMPDIR under the original cache and
CARGO_BUILD_JOBS=2. Only x86_64 GNU was executed; all other targets remain compile
checks. The helper fingerprint matches rust/target/debug/native-step-test-helper:
`c63631826ed9f0d5dea6fda62ec564db85d829ce7a3f1b8ccc76acf57000e15d`.

Pre-fix evidence is preserved against original helper
`672e40006fe72bc4674502e919f252ba01689e8da80353faa8638ab6d2ffa303`:
`before-final.jsonl` exits 1, 33 pass/22 fail records (including parent failures).
It independently fails multi-event admission, carrier-absent bytes with partition
mode (isolates omission), nonzero version metadata with key mode/carrier present
(isolates version), and malformed edge/readback classifications. It also exposes
prior overly strict readback for valid missing/lowercase/non-strict/zero-value
items. SHA256 `0f47cc484d6511e0122f7985481150f769e55b7c743ce89904e22851ef0a24d2`. Exact argv and failing case names are in its command
and result JSON files. Earlier before.jsonl/before-isolated.jsonl are retained,
as is after-focused.jsonl (55 passes, zero failures/skips). Initial clippy.log
exits 101 on two collapsible-if lints; repaired with let chains, final clippy
passes. Initial and final build logs remain; no failed test log was overwritten.

`final-source-sha256.txt` fingerprints changed sources, helper/build inputs and
all 12 preserved fixtures/original replay files. All 12 still match the original
manifest; production Go/Lua remains unchanged. Source comparison confirms only
enqueue.rs, runtime.rs, native_enqueue_integration_test.go and one projection
assertion in recovery_native_enqueue_test.go changed during this repair; earlier
helper/json/build/replay work is preserved. `git diff --check` and affected gofmt
checks pass. `plan-status.log` exits 1 solely on the unchanged run-ai-debugging
status, still untouched. `evidence-sha256.txt` records the local log/summary/source
manifest hashes. Final Go log hashes:

- final-owners-False.jsonl: `26e3bbf07b57f0d1f70d5fde8d050bfed9c684e00b4ef3161f28ebd01d861bed`
- final-replay-False-False.jsonl: `537515078bcc6e930a51b740c9ce130df599e8f412ddfcd55fcb1f2711753c9f`
- final-replay-True-False.jsonl: `e005371820f6c1a187234f7e25ccc7cf06fd5829e12fb51b1bd62ee107d8ada2`
- final-owners-True.jsonl: `6d945cedae1a27eca4bed5c09f61504cb839a5bbb510a64d7e51315adeae581f`
- final-replay-False-True.jsonl: `1ad0478040f8addc38829db9f9c06a8327339efc8e4943dec73335633869c937`
- final-replay-True-True.jsonl: `ee6e08fa370d62127a7fdefb8e2a46af23137313f1bd7aa0eaa20a218b452cf9`

At this repair handoff, fresh independent **Luna + Sol reviews were pending**;
none were delegated in that resume. Its working tree was uncommitted and no PR
had been opened. The following acceptance record supersedes only that pending
review state; it does not replace the earlier failures or hashes.

## Independent review and parent acceptance (2026-10-27)

The earlier missing-helper P2 false pass is closed. The repaired rejection test
first executes a positive preparation probe against an existing helper, then
requires exact exit status 2, `invalid helper input\n`, empty stdout, and zero
socket commands/connections. Independent Sol review 02 accepted this repair.
Its preserved old-helper negative controls reproduce the four native parity
defects described above; the earlier false-pass log and all repair/failure hashes
remain in this document and the cache evidence.

Independent Sol review 03 passed 276 Go test records across ordinary native
interoperation and fresh-owner replay in both queue modes, normal and race, plus
five Rust tests. There were zero failures and zero skips. Sol also verified the
source hashes and all 12 unchanged baseline fixture/replay files. The exact
summary is `/home/ashutosh/.cache/native-step-enqueue/sol-review03-1e22clw2/summary.json`.
Its SHA256 is `e2358a43fea5af34fb075fae69af477b12c7b5c18fa5cdbf5579f505c80b8fd2`.

Independent Luna review 02 passed five Rust tests, Rust formatting and clippy,
and targeted authenticated Valkey cases in both queue modes (8.714s and 8.508s).
That lane did not rerun Go replay or race selections. Its rebuilt helper hash
matches the accepted Rust 1.94 helper fingerprint recorded above. Author evidence
is separate: 926 focused Go normal/race pass records and the Rust formatting,
clippy, tests, helper build, and four target compile checks. Only x86_64 GNU was
executed; the other three targets are compile-only evidence.

The parent accepted the bounded scope: ordinary single, non-AI StepRun native
SaveStep followed by native discovery enqueue, with fresh Go readback/scanner
replay under normal, precommit-failure, and lost-ACK cases. Go does not perform an
effect on behalf of the native implementation. This establishes no production
wiring, atomic save-plus-enqueue, exactly-once behavior, process-crash recovery,
or active-lease recovery. Full-response compaction before the 4 MiB limit,
licensing/distribution, lifecycle, fencing, platform execution, and performance
gates remain open. Phase 1 remains partial, Phase 2 open, and the bridge unset;
the prior FFI no-go remains in force. No benchmark or broad-suite rerun was part
of this acceptance.

## Delivery (2026-10-27)

[PR #18](https://github.com/ashutoshpw/inngest/pull/18),
`internal(execution): add bounded native StepRun discovery enqueue`, is open
from `t3/native-step-enqueue` into `t3/native-step-save-adapter` (PR #17). Its
opening implementation commit is `927fc1869c99332e79ff3a62e8822cc7fd957ecc`;
the PR base resolved to `928af60e182e7dd7b679c5ac819b1fa74107cc82`. The opening
snapshot was non-draft and mergeable, with no review decision or check runs
reported. T3 links PR #17 and #18 to this delivery thread and parent thread
`c37dccc0-74a6-4b56-bc19-0ffcf321cc19`. This delivery record advances the PR
head after the implementation commit. Release note: None. Migration note: None.
