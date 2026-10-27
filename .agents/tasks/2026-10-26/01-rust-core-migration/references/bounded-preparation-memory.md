# Bounded preparation buffers (2026-10-27)

## Scope and base

Assigned T3 worktree `/home/ashutosh/.t3/worktrees/inngest/t3-6f28906c`, branch
`t3/native-prepare-memory-bound` (T3 renamed its initial `t3/6f28906c` branch
during setup; the reflog records that rename before our fast-forward). Initial HEAD `a4592cde4` was clean, with zero unique commits
(`git rev-list --left-right --count HEAD...e840eff6a84ecd83bbb46e87696d06007b24f2c9`
reported `0 33`). `git merge --ff-only e840eff6a84ecd83bbb46e87696d06007b24f2c9`
succeeded in place. Implementation base is PR18 `t3/native-step-enqueue` at that
exact SHA; the assigned worktree binding was preserved. No agent branch switch or
rename occurred. At the initial review handoff, changes were left uncommitted for
independent review. Luna review 01 found no correctness blockers and the parent
accepted the fix. This delivery commits the accepted implementation and its
documentation; no benchmark or production Go behavior change is included.

PR18 provides separate preparation/SaveStep and enqueue adapter operations,
orchestrated by Go tests. It is not a complete native response owner. This
package adds no opcode, dependency, transport, sequence or lifecycle behavior.
The [SaveStep](native-save.md) and [enqueue](native-enqueue.md) references retain
historical delivery and review evidence; this reference supersedes their earlier
preparation-compaction limitation. The helper's existing 32 MiB cap+1 repair is
unchanged.

## Guarantee and ordering

`prepare_response` reserves the nine bytes of `{"data":` plus `}` before
compaction. For data, the retained compacted prefix has at most **4,194,295 bytes
(4 MiB minus nine)**. Appends request no vector capacity beyond that limit; tests
observe both length and capacity, including expansion and overflow. Eligible
output gets an envelope allocated for its exact length, at most **4,194,304 bytes**.
Oversized output never constructs the envelope. These are per-buffer bounds;
the compacted prefix and accepted envelope can coexist during preparation.
Empty opts/metadata admission uses bounded two-byte compaction, rejecting an
oversized prefix rather than treating it as a complete empty value.

Full raw-response JSON validation still runs first, including suffixes past the
output cap. The compactor validates its data slice before emitting bytes and
continues scanning emitted compacted bytes after retention saturates. An
11-byte rolling suffix detects the same literal `totalUsage`/`inputTokens`
windows as the old complete compacted vector, including markers crossing or
following the retained prefix. Lexically escaped spellings remain unchanged.
Admission configuration still precedes raw validation, as before. For admitted
configuration, malformed JSON remains `InvalidJson`; unsupported fields/AI
triggers remain `Ineligible`; output overflow precedes state-size overflow.
State equality, configured-zero default and absent state-limit behavior remain.

The Go owners inspected were `GeneratorOpcode.Output`/`Validate` in
`pkg/execution/state/opcode.go`, validation in `driver/httpdriver/parse.go`, and
`goResponseEffects.prepareStep` in `executor/response_effects.go`. Go validates
opcode output limits before state-size preparation. The prototype's conservative
AI eligibility rejection is its own existing contract, not an assertion that Go
returns `Ineligible`. Tests distinguish those boundaries.

**No whole-process memory cap:** raw input, helper deserialization, identifiers,
unsupported strings/field names, parser work and allocator internals are outside
this guarantee. Parser/input memory and CPU can still depend on input; JSON is
still scanned before retention saturates. Enqueue's existing unrestricted
serialization compactor retains its original API. No parser redesign or input
length shortcut was introduced. Whitespace can shrink raw input and escaping
can expand it, so raw length alone cannot decide output eligibility.

## Test authoring gate and regression proof

- Independent contracts: unchanged seven literal preparation answers; actual Go
  `GeneratorOpcode.Output()` bytes and `Validate()` limits; fixed native error
  precedence; observable retained buffer length/capacity.
- Credible regression: full compacted retention before rejecting, wrong envelope
  allowance, raw-length rejection, missed late AI marker, changed lexical escaping
  or error ordering. Existing size tests rejected large output but could not
  observe its retained buffer; a private unit test now does so without a public
  production seam. Successful exact-limit bytes and expansion are compared at
  the helper boundary against Go, not Rust-generated expectations.
- `TestNativeRustBoundedPreparation` adds ten subcases: exact limit, plus one with
  state overflow, HTML expansion, U+2028/U+2029 expansion, both AI markers after
  the cap, malformed suffix with AI, malformed late escape, unsupported opts after
  large data, and raw whitespace over 4 MiB compacting to a small ordered object.
  Existing default-state and literal-corpus tests remain the primary owners of
  those contracts. Existing real-Valkey preparation/save and enqueue integration
  verifies the shared compactor's callers without a new replay campaign.
- The private compactor test checks independent literal bytes, capacities and
  overflow at limits 0/1/17/exact-minus-one/exact, AI across all split positions,
  escaped non-trigger spelling and malformed trailing JSON. A temporary mutation
  replacing bounded retention with unconditional append failed on the retained
  bytes assertion (exit 101). Before/after restoration SHA-256 of `json.rs`:
  `03e21f85a2e69e8a8d500cc2780a7a59eebbc7452d84813ac5487261574a14ab`.
  Restored Rust tests pass. An earlier mutation attempt failed on subtraction
  overflow; it was not counted as the intended assertion proof.

## Validation evidence

Local artifacts are under `/home/ashutosh/.cache/bounded-preparation/`, including
literal argv/environment/cwd sidecars (`*.command.json`), exit/time receipts
(`*.result.json`), raw logs and the validation driver. These are local review
artifacts, not durable checked-in logs. Commands and final counts follow below.

Go uses cached `go1.26.4 linux/amd64`, `GOTOOLCHAIN=local`, `GOMAXPROCS=2`, `-p=2`,
and both `GOTMPDIR` and `TMPDIR` at the cache's `tmp/`. Native helper is the bound
worktree's `rust/target/debug/native-step-test-helper`. Existing authorized
`sudo -n -u ashutosh -g docker env` enables the unchanged authenticated real-Valkey
helper; only its owned containers are cleaned up. No unrelated container is
modified. Runtime execution is x86_64 GNU only.

Rust commands from `rust/`, pinned `cargo +1.94.0`, `CARGO_BUILD_JOBS=2` and
cache-local `TMPDIR`:

```sh
cargo +1.94.0 fmt --all --check
cargo +1.94.0 clippy --locked -p native-step-state --all-targets --features test-helper -- -D warnings
cargo +1.94.0 test --locked -p native-step-state --all-targets --features test-helper
cargo +1.94.0 build --locked -p native-step-state --features test-helper --bin native-step-test-helper
cargo +1.94.0 check --locked -p native-step-state --all-targets --features test-helper --target TARGET
```

All pass: six Rust tests (one new buffer test, four preparation tests and the
unchanged helper cap+1 test), zero failures/ignored. Each of x86_64 GNU/musl and
aarch64 GNU/musl passes target compilation; the other three targets have no
runtime evidence. Restoration reruns all six Rust tests successfully.

Initial failures (documented here and in available logs): the first compile exposed enqueue's shared
compactor API, which is now preserved by a wrapper; the first Rust size run caught
an eight-versus-nine-byte envelope allowance, corrected before Go checks; the
first Go HTML test used a floor repetition count whose output was five bytes
below the limit, corrected by adding one repetition. No production semantic
change was made to accommodate these test failures.

`scripts/check-plan-status.sh` exits 1 solely for the known unrelated
`docs/plans/run-ai-debugging.md` status `Implemented — pending staging review`;
that file remains unchanged. The existing 12-file baseline manifest verifies
successfully; all fixture/replay bytes remain unchanged from base.

Final Go command, run once normally and once with `-race` for each `KEYS=false`
and `KEYS=true` (the failed first false/normal attempt is superseded by
`go-false-normal-fixed.log`):

```sh
sudo -n -u ashutosh -g docker env \
  GOTOOLCHAIN=local GOMAXPROCS=2 \
  GOTMPDIR=/home/ashutosh/.cache/bounded-preparation/tmp \
  TMPDIR=/home/ashutosh/.cache/bounded-preparation/tmp \
  EXPERIMENTAL_KEY_QUEUES_ENABLE=KEYS \
  NATIVE_STEP_TEST_BINARY=/home/ashutosh/.t3/worktrees/inngest/t3-6f28906c/rust/target/debug/native-step-test-helper \
  /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -p=2 \
  -tags=native_step_integration ./pkg/execution/state/redis_state ./pkg/execution/executor \
  -run '^TestNative(Rust(BoundedPreparation|OutputAndDefaultStateLimits|JSONOwnerParity|PreparationInterop|EnqueueInterop)|StepPreparationAccountingThreshold)$' \
  -count=1 -timeout=180s
```

| Final log | Pass records | Seconds (including Go build) | Exit |
| --- | ---: | ---: | ---: |
| `go-false-normal-fixed.log` | 129 | 28.926 | 0 |
| `go-false-race.log` | 129 | 129.438 | 0 |
| `go-true-normal.log` | 129 | 26.411 | 0 |
| `go-true-race.log` | 129 | 55.159 | 0 |

Each final invocation has 126 Redis-state and three executor test/subtest passes:
**516 total, zero failures/skips**, with no Go race report. These are focused
selections, not full suites; `-race` instruments Go, not the external Rust helper.
Valkey is the unchanged helper's `valkey/valkey:8.0.3`, linux/amd64. The 14 unrelated
running container IDs/names compare identically before/after all checks. The
initial false/normal attempt had 127 passes and two failure records (HTML child
and its parent); it is not counted among final successes. The successful false
race invocation compiled the corrected test. No recovery/benchmark campaign ran.

SHA-256 fingerprints:

| Artifact | SHA-256 |
| --- | --- |
| json.rs | `03e21f85a2e69e8a8d500cc2780a7a59eebbc7452d84813ac5487261574a14ab` |
| lib.rs | `9772c89862f898cfaf8b6a6ae0a016c42649f2b7419caf7c654044bc7d98c814` |
| Go limits test | `625106eb2b635082bae6de1394da4e3a28020655c33cc2ba665e91111f824e4c` |
| Native helper | `0058ee3228ea396b028a6dda481df7ac823dd7d15ca9d54395cd19fc158b826d` |
| Source/base comparison manifest | `5408fe33d32f9326d8041845ce0cf62c7a84be6a045dd245958886233e1ff252` |
| Local evidence manifest | `cd7c0b881e6bf4ec1b37170b6b8fced7f4b3e1549d95103b710ef0b303bb0a19` |
| Preserved 12-file baseline manifest (prior enqueue cache) | `f1bb74855723a911ceb5424e92503281c30ad1428cfb48e75e07ff597baf4192` |

`source-manifest.json` compares the original 12 baseline files directly to HEAD,
and verifies unchanged helper/enqueue/runtime, Cargo lock/dependencies and the
inspected Go owners. Production changes are confined to Rust `json.rs` and
`lib.rs`; Go changes are test-only. `evidence-sha256.txt` covers local logs,
commands/receipts, validation driver, mutation/restoration and baseline/container
checks; failed attempts are retained where logs were not superseded. The first
compile's log was overwritten by the successful final Rust run; its diagnostic
is documented above. `git diff --check` and focused formatting pass.

## Remaining gates

Luna review 01 independently passed six Rust tests, Rust formatting/helper checks,
and focused Go actual-`Output()`/JSON parity in normal and race modes, with no
correctness blockers. The parent accepted this increment, delivered in [PR #19](https://github.com/ashutoshpw/inngest/pull/19).
The Go race run instruments Go only, not the Rust helper process. The parent's
acceptance does not change this increment's scope or the separate author evidence
above. Current FFI SCREEN_NO_GO stands; Phase 1 remains
partial, Phase 2 open and bridge choice unset. Licensing reconciliation,
other-platform runtime qualification, production admission/wiring,
lifecycle/fencing, uncertain effects, crash/lease recovery and rollout gates
remain open. There is no owner benchmark, performance or complete native
response-owner claim.
