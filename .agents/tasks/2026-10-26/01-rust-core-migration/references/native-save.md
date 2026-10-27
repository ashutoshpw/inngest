# Native ordinary StepRun preparation and SaveStep

## Scope and provenance

Follow-up in [native enqueue](native-enqueue.md) repairs the helper transport cap
and adds bounded discovery. The evidence and original scope below describe the
accepted SaveStep delivery as of PR #16, when preparation compaction was still
unbounded. The accepted [bounded preparation memory increment](bounded-preparation-memory.md)
supersedes that specific compaction limitation; input, parser and whole-process
memory limits remain open.

Parent accepted this bounded prototype after two independent reviews; it remains
internal and has no production caller.
Dedicated worktree
`/home/ashutosh/.t3/worktrees/inngest/t3-844aeeb4`, branch
`t3/native-step-save-adapter`, fast-forwarded without unique commits to
`2cb76b3d07587eeab4c2c105c86842cdf81d8370` (PR #16 head and stack base).
The resumed work preserved the partial implementation and prior logs after an
earlier provider-capacity failure; no native reimplementation was needed.

One accepted ordinary non-AI single StepRun for a precreated isolated run.
Native output preparation, size validation and native SaveStep only. Go setup
and fresh independent Go reads run outside native effects. Discovery/enqueue,
worker/SDK HTTP, trace/metadata generation and end-to-end native replay remain
open. No FFI or Go effect callbacks, enqueue, lifecycle, SDK, activation or
performance claim. Preserve SCREEN_NO_GO; this is the accepted alternative
native-owner experiment. Phase 1 partial, Phase 2 open, bridge unset.

## Independent review and acceptance

The parent accepted the bounded prototype with no blockers from either review.
Sol's independent interop run passed 240 records in normal mode (19.408 s) and
240 in race mode (26.423 s). That review selection excluded the long retry
exhaustion case; the author's full resumed normal/race logs independently record
that case passing. Luna-max independently passed all four Rust unit tests and
the Go output differential plus fresh-save and committed-reply Valkey cases.
These review scopes are separate from the author's broader local validation below.

## Test authoring gate

1. Independent contracts: the unchanged seven literal Go preparation outputs,
   21 SaveStep rows, complete raw store inventory and fresh Go load semantics.
2. Regressions: number/order changes, wrong keys/accounting/pending, hidden
   network replay or SCRIPT LOAD fallback, lost-ack retry and conflict heuristic.
3. Existing Go owners cannot exercise the Rust client or actual socket failures;
   reuse their fixtures, setup and raw-reader helpers instead of copying an oracle.
4. No test-only production injection hook: private TCP relay controls actual
   Rust sockets, real Valkey executes commands, independent Go reads prove effects.

## Checklist

- [x] Verify exact base, clean status, T3 binding and read accepted references/owners.
- [x] Implement preparation, eligibility, typed keys/config and native adapter.
- [x] Runtime qualification and shared fixture interoperability, normal/race.
- [x] Rust fmt/clippy/locked tests and four target checks.
- [x] Record evidence, unresolved limits and leave uncommitted for review.
- [ ] Parent's independent Luna + Sol review and acceptance.

The first checklist entry describes the original clean base. The resumed checkout
was intentionally dirty and was preserved, on the same branch and exact HEAD.

## Implementation and independent boundaries

`rust/crates/native-step-state` is unpublished and has no production caller.
Its feature-gated `native-step-test-helper` is a one-request integration artifact,
not a released CLI, FFI callback, queue consumer or effect bridge. `PrepareSave`
passes raw accepted-response bytes to Rust for both preparation and persistence;
Go creates only initial state and independently reads the result. `Save` separately
exercises SaveStep's opaque-byte contract and explicit metadata-size input.

Preparation accepts exactly one StepRun with request version 2, no force-plan,
disabled trace ownership and no AI/SDK/experiment metadata. Unsupported response
fields, nonempty opts and duplicate opcode fields fail admission. Payloads with
the current Go AI extractor's `totalUsage`/`inputTokens` byte triggers are
conservatively excluded. This is restricted admission, not an implementation of
AI extraction, trace generation or arbitrary SDK response support.

The iterative JSON scanner preserves raw number lexemes, object/duplicate-data-key
order, raw invalid UTF-8 and escaped surrogate code units, while matching Go's
whitespace removal, HTML and U+2028/U+2029 escaping and 10,000-level depth limit.
Data is never decoded into a floating-point JSON value. The seven unchanged Go
literals remain the primary oracle; additional differential cases call the real
Go `GeneratorOpcode.Output` and JSON decoder. Four Rust tests protect literal
preparation, admission/limits, scanner edges and already-cancelled admission.

At the accepted PR #16 commit, preparation compacted the full accepted response
before checking the prepared output limit; output over 4 MiB then rejected before
state-size validation. The later [bounded preparation memory increment](bounded-preparation-memory.md)
limits retained compacted data to 4,194,295 bytes and prepared output to 4,194,304
bytes, with each buffer bounded independently. The buffers can coexist. Full raw
response validation and AI-marker scanning still continue past the cap, while
input, parser, CPU and whole-process memory remain outside the guarantee.

The opt-in test helper still reads at most a 32 MiB stdin prefix without probing
for or rejecting an extra byte, so an oversized transport can be truncated before
JSON parsing. This input-cap limitation remains open. Before production wiring,
reject oversized input explicitly and assess parser/process memory limits. State
validation checks loaded size plus prepared bytes, accepts equality, uses 32 MiB
for configured zero, and is absent when the callback-equivalent option is absent.
Size errors expose the Go class/message/standard-error fields without pretending
to preserve Go sentinel or error-chain identity across processes. This is an
already-loaded observation, not a concurrent reservation or full state lifecycle
owner.

Typed run/function/account/environment/app identity and explicit layout/prefix
generate the existing five KEYS in unchanged order. Both `NeverShard` and
`AlwaysShard` layouts and `estate`/`native:state` prefixes are exercised. Only one
explicit authenticated endpoint is selected; no native physical shard discovery,
cluster redirection or account-placement policy is implemented. Run keys use the
run hash tag in sharded layout; account identity remains available for the host's
endpoint choice. Empty/brace-bearing prefixes and noncanonical identifiers reject
configuration rather than silently constructing unsupported keys.

The build derives Lua from the existing repository source and checks its pinned
SHA-256 and length; no generated/copied script is checked in. The accepted source
has no includes, so raw and expanded bytes agree. A future include/source change
must be requalified. Existing Go expansion and manifest assertions remain the
independent authority: 1,611 bytes, SHA-1
`2c371556f31bb03e150253bbd588a9a34255061a`, SHA-256
`402c8d9e0a6de6de2d6117c0b69f5a6e4823410c05437dc46c30a7aa0d3b9409`.
`build.rs` currently requires the repository tree and `sha256sum` (Linux build
qualification only). Exact wire KEYS/ARGV and script bytes are checked by the relay.

## Persistence cases

`TestNativeRustSaveStepInterop` consumes all 21 unchanged fixture rows: seven
scenarios with pending `[]`, `[s]`, `[s, held]`, repeated for both layouts and
both prefixes (84 subtests). A new Go adapter and cache-disabled connection read
the full raw key inventory, output, stack, loaded metadata and pending state.
The shared Go fixture projection was extracted from the existing Go test, without
changing fixture answers. The Go adapter's own test remains independently run.

- Fresh: one output, one stack push, state size +25 and step count +1; only `s`
  is removed from pending, and the result reflects remaining `held`.
- Identical separate request: original `ErrIdempotentResponse` class and current
  pending after real Go `SavePending`; no accounting/stack/output change.
- Conflict: `ErrDuplicateResponse`, false pending result, original output retained.
- Commit/retry: real Lua commits once; the relay withholds the genuine reply
  until an independent fresh read proves the commit while the caller is live.
  Closing that socket forces one adapter retry; the idempotent second reply is
  swallowed according to Go's attempt heuristic, with current pending preserved.
- Commit/no retry: same independent commit proof and reply loss, one attempt,
  `ErrMaxAttemptReached` and false pending result despite a durable output.
- Preexecution/conflict: first command is dropped before Valkey executes it;
  a conflicting second response is swallowed just as Go does. This heuristic
  explicitly does not prove that the requested conflicting bytes committed.
- Input/metadata: retained step input subtracts its byte length from output
  growth; explicit positive metadata delta is added once. This does not prove
  generation of native AI/SDK metadata.

Native outcomes contain only replies actually acknowledged to the native caller;
the relay records the lost reply separately. `Confirmed`, `NotApplied`,
`MayHaveCommitted` and `NotSent` describe evidence, not exactly-once guarantees.
After each native result a fresh Go idempotent SaveStep call also checks the real
Go sentinel and current pending without another persistent mutation.

`TestNativeRustPreparationInterop` consumes all seven unchanged literals across
the same four layout/prefix combinations (28 subtests). Each tests one-byte-below
state rejection, equality acceptance, actual native save, complete raw state and
fresh Go output/size. Missing/null, large integer, exponent, negative zero, ordered
object and string escaping answers are unchanged. `TestNativeRustJSONOwnerParity`
adds 15 real-Go decoder/output edge comparisons; `TestNativeRustOutputAndDefaultStateLimits`
uses independent Go constants/validation to guard the exact 4 MiB/32 MiB boundaries.

## Redis runtime qualification and limits

Exactly `redis = 1.8.0`, defaults disabled, `script` only. Locked metadata has
only that Redis feature and no Tokio/TLS/OpenSSL/C build dependencies. All prior
lock entries are unchanged; added checksums/license/MSRV declarations are in
[the dependency inventory](native-save-dependencies.json). Declarations are
upstream metadata, not a license reconciliation or security audit.

The runtime uses one synchronous `redis::Connection`, never a reconnecting manager
or `Client` as the query connection. Local dependency source confirms one packed
send/read per command. It explicitly issues `EVALSHA`, then exact `EVAL` only for
`NOSCRIPT`; no `Script::invoke` or `SCRIPT LOAD` fallback. A real flushed script
cache proves those two wire commands count as one adapter attempt. Subsequent
successful calls use `EVALSHA`. The relay rejects unexpected non-AUTH commands.

The adapter owns the Go `v2.SaveStep`/`util.WithRetry` rules: at most ten attempts
(one when disabled), delays 100/200/400/800/1600/3200/6400/10000/10000 ms,
nonretryable duplicate/idempotent replies, and swallowing either class only after
an earlier attempt. Error reporting is classified, not a synthetic Go error value.
No transparent client write retry occurs; failed sockets are dropped before the
adapter may explicitly reconnect.

`TestNativeRustRuntimeQualification` exercises actual authenticated Valkey through
the private TCP relay, with independent no-mutation reads for unsuccessful cases:

- `before`: drop one script before execution, disabled retries => one command.
- `read-stall`: authenticate, receive script, withhold execution/reply; 60 ms
  configured read inactivity timeout, one command, no Valkey mutation.
- `write-stall`: authenticate then stop reading an 8 MiB opaque write beyond TCP
  buffers; 60 ms configured write timeout, no complete script forwarded.
- `auth`: incorrect password rejected by Valkey before a script is sent.
- `connect`: refused private endpoint, no script.
- `handshake-stall`: the relay stalls before forwarding AUTH to Valkey; the native
  client returns a bounded setup timeout without sending a script.
- `budget`: expired context admits zero attempts/connections.
- `backoff-budget`: first real drop followed by deadline expiry during backoff;
  no second connection or command.
- `reconnect`: actual preexecution socket drop, two AUTHs/two script attempts,
  exactly one command forwarded/committed, fresh read proves state.
- `before-always`: ten actual reconnects/script drops, no forwarded script,
  real observed backoff intervals and exhaustion. Its ~32.7 s duration is accounted
  for separately in the original idempotency key's TTL comparison.
- `authenticated-binary-safe`: real ACL username and password with NUL/CRLF/URL
  metacharacters, non-UTF-8/NUL/CRLF step/output bytes, native write and independent
  raw Go reads; AUTH arguments are never retained in logs/inventory.
- `eligibility-before-effects`: tracing, AI, SDK metadata, experiments, forced
  plan, unsupported request version and opcode metadata each open zero sockets.

`TestNativeRustConnectTimeout` fills a private Linux accept queue, independently
probes that a SYN cannot complete, then observes a native connect timeout with a
60 ms setting, no save and one attempt. This qualifies this host/kernel's actual
TCP path; it is not a universal kernel/TCP-stack timing bound. No firewall/routes
or unrelated containers are changed.

Addresses must be literal IP:port, avoiding unbounded DNS/address failover. Setup
and socket operations use remaining budget capped by configured timeouts.
Cancellation is cooperative between blocking phases and during backoff; blocking
I/O is not preempted. Socket timeouts are inactivity limits, not a total or
dribble-proof deadline. Postcommit ACK suppression keeps the caller process and
context live; it is not caller cancellation or a synthetic successful write.

## Reproducible validation

Run from the dedicated worktree. Rust commands run from `rust/` under pinned
Rust 1.94.0; the native runtime tested here is Linux x86_64 GNU only:

```sh
cargo fmt --all -- --check
cargo clippy --locked -p native-step-state --all-targets --all-features -- -D warnings
cargo test --locked -p native-step-state --all-features
cargo build --locked -p native-step-state --features test-helper --bin native-step-test-helper
```

Separately run `cargo check --locked -p native-step-state --all-targets --all-features --target TARGET`
for `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`,
`aarch64-unknown-linux-gnu`, `aarch64-unknown-linux-musl`. All four checks pass;
cross-checks do not establish final links or native arm64/musl execution.

Final focused Go command, separately without and with `-race`:

```sh
sudo -n -u ashutosh -g docker env \
  GOTOOLCHAIN=local GOMAXPROCS=2 \
  GOTMPDIR=/home/ashutosh/.cache/native-step-save/tmp \
  TMPDIR=/home/ashutosh/.cache/native-step-save/tmp \
  EXPERIMENTAL_KEY_QUEUES_ENABLE=false \
  NATIVE_STEP_TEST_BINARY=/home/ashutosh/.t3/worktrees/inngest/t3-844aeeb4/rust/target/debug/native-step-test-helper \
  /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -p=2 \
  -tags=native_step_integration \
  ./pkg/execution/executor ./pkg/execution/state/redis_state \
  -run '^(TestNative|TestResponseContinuationContract$|TestStagedBatch|TestQueueItemPersistenceCompatibility$|TestRunStatePersistenceCompatibility$)' \
  -count=1 -timeout=180s
```

The native integration build tag is opt-in. Selecting it requires an absolute
existing `NATIVE_STEP_TEST_BINARY` and working Docker; missing prerequisites fail
instead of skipping. No benchmark, full E2E, worker/SDK HTTP or production routing
was added or run. Existing Go queue-oracle subtests explicitly exercise both queue
modes; the native state adapter has no queue mode or enqueue owner.

Real Valkey 8.0.3 Linux/amd64, authentication and AOF enabled by the existing
`StartValkey` helper. Image ID
`sha256:88251207ac13df6d4bd4d3a77e6953ca3b90e0e5cc6acb6b3ece0fc2f9091289`,
digest `valkey/valkey@sha256:4e78d8ccab4d8978afca140a3dd75b4e45d683d1e51602ee71e976bcc2bd939e`.
No Valkey restart/fsync/disk-loss guarantee is established.

## Evidence and review handoff

Local evidence lives in `/home/ashutosh/.cache/native-step-save/`. The resumed
commands/logs use `resume-` names, preserving every earlier log, including failed
attempts. `resume-go-commands.json` and `resume-rust-commands.json` record exact
argv/cwd/exit status; `resume-validation-summary.json` records final counts,
per-case outcomes and log hashes. `resume-source-sha256.json` pins the source and
helper artifact used for this handoff. These local logs are not portable committed
artifacts; the unchanged repository fixtures and tests are the repeatable oracle.

Final resumed results (2026-10-27): normal executor **173 / 3.829 s**, Redis-state
**267 / 57.963 s**; race executor **173 / 5.907 s**, Redis-state **267 / 64.831 s**.
Every package passed with zero test failures/skips and no race report. Rust fmt,
clippy with warnings denied, all four Rust tests, helper build and all four target
checks passed. Native preparation/persistence/fault tests ran against actual
authenticated AOF Valkey, not just a compile or fake-client check.
Go formatting and `git diff --check` pass. `scripts/check-plan-status.sh` still
exits 1 solely for unchanged `docs/plans/run-ai-debugging.md`, whose status is
`Implemented — pending staging review`; see `resume-plan-status.log`. This is the
previously recorded repository plan-status issue, not a new migration failure.

- `resume-normal.jsonl` SHA-256:
  `d37d205e3168e8fbab828011bc7f39b5d2c677b16e4aa3b7947765a85981eed3`.
- `resume-race.jsonl` SHA-256:
  `c99313203872ebae098c5e535c1181c4e789b46ab5c2512a7a06f0b6fca3c008`.
- Native helper SHA-256:
  `56f356d110f9a8cd4cce214cf1ee2c2c93be3da5aad4da2fa3875a0a13874ac7`.

Earlier `interop-initial.log` failed at helper input admission (`invalid helper
input`); `interop-02.log` passed after the transport repair. No fixture answers
were changed. Existing final/delivery logs and their old summary remain historical;
they are not substituted for the resumed final-source validation.

Discovery/native enqueue, native end-to-end replay, generated tracing/full
metadata, lifecycle/fencing/active-lease recovery, concurrent owner semantics,
platforms, performance, production wiring and release remain open. Runtime
execution was qualified only on Linux x86_64 GNU; the other three targets were
compile-checked, not executed. Cancellation remains cooperative around blocking
phases, and socket inactivity timeouts do not impose a total or dribble-proof
deadline. Dependency metadata is recorded, but license reconciliation remains
open before distribution. Original FFI/SCREEN_NO_GO evidence is unchanged. Broad
Phase 1 stays partial, Phase 2 open, bridge unset.

## License gate

Root SSPL and workspace Apache-2.0 metadata/future grant remain unresolved.
Preserve metadata and original Lua/license provenance. No distribution rights
are asserted; distribution requires owner/legal resolution. No copied Lua is
relicensed. The build must derive the pinned script from repository sources.
