# Native-step fresh Go runtime replay

**State:** parent-accepted Go-only baseline after independent Luna review found no blockers. [PR #16](https://github.com/ashutoshpw/inngest/pull/16) is open as a stacked Go-only layer. This does not implement or qualify a native adapter.

**Base and binding:** clean pre-existing isolated T3 worktree
`/home/ashutosh/.t3/worktrees/inngest/t3-d7790020`, branch
`t3/native-step-go-baseline`. Initial HEAD `a4592cde49cab4c4a1afc5488cf39345514d5ed7`
had zero unique commits versus PR #15's fetched head. Fast-forwarded in place to
`6dcd3b71fa4ba5dcaa98c1562c5f072ecc30d6ee`, preserving the T3 binding.
The supplied `ff6dcd...` was interpreted as “ff” plus this exact PR #15 head;
the fetched branch and full object identity were verified before edits.

## Scope and test-authoring gate

Parent accepted the native-owner experiment amendment. Current FFI-boundary
SCREEN_NO_GO remains stopped. This increment extends the existing recovery owner
with three ordinary, non-AI single-StepRun scenarios; it adds no production seam,
Rust tag/source/dependency, benchmark, or general recovery framework.

1. Protect literal saved output, wrapper, action stack, negotiated request version,
   identity, real discovery persistence and normal fresh-owner scanner replay.
2. Credible regressions: dropped/changed checkpoint repeats a deliberately
   non-deduplicated external INCR; missing discovery/retry loses progress;
   retry consumes the original attempt or leaves a lease; replay sends the wrong job.
3. Existing persistence owners stop at SaveStep/discovery/producer retries. Existing
   recovery owns precommit failure followed by a Sleep. This table owns terminal
   replay across normal, precommit-failure and committed-response-loss boundaries,
   reusing that runtime/HTTP driver setup and its literal projections.
4. No production seam: test-private forwarding queue/state observers and a public
   shard Lease boundary control A's admission. B has fresh, unwrapped owners.

The explicit implementation/test-authoring instruction authorizes this edit shape;
the skill's generic stop-for-selection step does not require another approval.

## Checklist

- [x] Verify clean checkout, zero unique commits, exact baseline and T3 binding.
- [x] Read existing recovery, persistence owners, fixtures, task and authoring gate.
- [x] Three table scenarios; raw store, SaveStep and actual HTTP observations.
- [x] New and unchanged recovery tests separately, both queue modes, normal/race.
- [x] Record exact commands, outcomes, dependency provenance and limits.
- [x] Independent Luna review found no blockers; parent accepted the Go-only baseline.
- [x] Keep actual native adapter/runtime-client tests and release qualification open.

## Boundaries and retained limits

Only the SDK endpoint, SQLite file and authenticated AOF-enabled Valkey survive.
A drains and closes its loader/SQL handle, state/queue clients, executor/listeners,
driver/HTTP connections and scanner. B opens all owners and clock afresh, loads the
registered function without upsert, and normally scans durable work. No manual
enqueue, reconstructed captured job, fake successful writes, or fake future state.

No OS crash, active-lease recovery, fencing, concurrent-owner, exactly-once,
atomic SaveStep+enqueue, Valkey restart/fsync or disk-loss guarantee is claimed.
The prior P3 Lua-command-selection and response-generated whole-item-byte limits
in [native-step-go-baseline.md](native-step-go-baseline.md) remain unchanged.
Actual native dependency/runtime qualification, licensing, broader recovery, SDK
policy, native arm64 execution and production activation remain open; broad
Phase 1 is partial, Phase 2 open, bridge unset.

## Preliminary native Redis dependency qualification

Separate Luna research was accepted as **preliminary compile/metadata evidence
and a dependency recommendation only**: `redis` exactly `1.8.0`, with
`default-features = false` and `features = ["script"]`, for Rust 1.94 / edition
2024. Compile and metadata checks passed for x86_64 GNU/musl and aarch64 GNU/musl
targets. The selected feature set adds no Tokio, TLS, OpenSSL, or C build; the
crate reports MSRV 1.88 and BSD-3-Clause licensing. Sources: [redis 1.8.0 on
crates.io](https://crates.io/crates/redis/1.8.0), [redis 1.8.0 API
documentation](https://docs.rs/redis/1.8.0/redis/), and [its 1.8.0 script
implementation](https://docs.rs/redis/1.8.0/src/redis/script.rs.html).

Go parity requires low-level `EVALSHA`, then the exact expanded script through
`EVAL` after `NOSCRIPT`; `Script::invoke`'s `SCRIPTLOAD` fallback is not the
accepted equivalent. Explicit client attempts/retries remain adapter-owned.
Connect/read/write I/O timeouts do not provide a total deadline or cancellation
contract. No actual Redis runtime-client test, native arm64 execution, or
performance proof exists. This is reported accepted evidence; no local commands
or logs are claimed here. Native adapter implementation, runtime behavior and
release qualification remain open.

The root repository uses SSPL while the Rust workspace uses Apache-2.0; a future
license grant and its availability date are unresolved. No license files or
metadata changed. Owner/legal confirmation is required before distribution;
internal prototyping can proceed.


## Per-scenario evidence and implementation shape

The [existing fixture study](../../../../../tests/execution/executor/testdata/sdk-response-replay-v1/README.md#native-step-terminal-replay-extension-2026-10-27)
records the portable read recipes and lifecycle. The original Sleep test function
and its five response/request/state fixtures are unchanged. Only the existing
`makeExecutor` helper gains optional executor options for a forwarding SaveStep
observer; the shared request assertion accepts a literal per-scenario cursor.
No new general runtime or newExec harness is introduced.

- **Normal checkpoint/discovery:** A returns nil; original `16e6bgk5j2d2f` is
  normally dequeued, leaving a real producer-dedupe tombstone. Discovery
  `21swrikw69rwn`, generation 1, attempt 0, remains unleased. B's normal scanner
  dispatches that discovery and HTTP replay carries stack cursor 1. Terminal
  completion removes state and work, leaving positive bounded tombstone TTLs.
- **Saved, discovery fails before commit:** the independent raw pre-discovery
  observation already contains the exact 41-byte output and one stack entry.
  The original sentinel is returned before forwarding, and Execute's genuine
  AlwaysRetry error message is retained. Before drain, the real worker durably
  requeues the original at `1793016600000`, generation 2, attempt 0, no lease.
  B scans that persisted original, cursor 0; terminal cleanup leaves the
  original tombstone and no discovery tombstone because discovery never committed.
- **Discovery committed, ACK suppressed:** actual Enqueue returns nil before
  independent snapshot and one response suppression. The separate canceled
  command yields the original `context.Canceled` error; the caller context
  remains live. Execute returns real AlwaysRetry. Both the original durable
  retry above and one generation-1 unleased discovery survive A. B scans the
  discovery at 12:01 UTC while the original retry is still in the future,
  cursor 1; normal terminal finalization also removes that original retry.

All three require external INCR count 1, complete HTTP request identity/version/
checkpoint observations, action/stack before deletion, exact raw run/status
membership/scores and real ready/backlog placement. Each read opens a new
cache-disabled connection and closes it before returning. The q admission
wrapper forwards the actual original Lease and blocks discovery **before** its
real Lease; it never mutates a stored item. B has no admission wrapper.

The real HTTP path adds executor-generated HTTP timing metadata. These measured
values are not fixed fixture bytes or opcode SDK/AI/experiment metadata. The
forwarded SaveStep delta must equal independently read `metadata_size` after
SaveStep; the stored value must remain unchanged through discovery, A drain,
and B's pre-terminal observation. The fixed event-state input is 98 bytes and
post-save state is 139 bytes. Schedule's returned metadata has zero metrics, so
it is not used to generate the expected state size.

No whole-response queue-item encoding or Lua command-selection assertion was
added; prior P3 limitations remain exactly as accepted. Generated group UUIDs
are v4 and agree with enqueue context/store. Propagation schema is exactly
`tp/ts/dstp/dsid`, with fixed deterministic trace/root and checked generated span
relationships. Raw values are retained, rather than normalized from comparisons.

## Validation and provenance

Actual authenticated AOF Valkey 8.0.3 `linux/amd64`, image ID
`sha256:88251207ac13df6d4bd4d3a77e6953ca3b90e0e5cc6acb6b3ece0fc2f9091289`,
repo digest
`valkey/valkey@sha256:4e78d8ccab4d8978afca140a3dd75b4e45d683d1e51602ee71e976bcc2bd939e`.
The existing helper starts a fresh instance per scenario with authentication and
`--appendonly yes`. SQLite is opened with `Persist:true, ForTest:true` against
one surviving temporary directory and reopened on B; no upsert is performed by B.
Stores are not restarted. Go binary
`/home/ashutosh/.cache/go1.26.4/go/bin/go`, `go1.26.4 linux/amd64`.

Every test command uses this authorized environment prefix:

```sh
sudo -n -u ashutosh -g docker env \
  GOTOOLCHAIN=local GOMAXPROCS=2 \
  GOTMPDIR=/home/ashutosh/.cache/native-step-replay/tmp \
  TMPDIR=/home/ashutosh/.cache/native-step-replay/tmp \
  EXPERIMENTAL_KEY_QUEUES_ENABLE=false \
  /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -p=2 \
  ./tests/execution/executor \
  -run '^TestRecoveryReplaysNativeStepBaseline$' -count=1 -timeout=180s
```

Run this separately with `EXPERIMENTAL_KEY_QUEUES_ENABLE=true`, and separately
for `-run '^TestRecoveryReplaysPersistedSDKResponse$'`. The four race commands
are the same four separate invocations with `-race`. All final commands use
`-timeout=180s`; earlier normal runs used 120s.
No Rust tag/library/dependency, benchmark, full E2E or broader suite is required
or was run. No required case was skipped.

Eight final-source invocations pass: new table false/true normal **5.682/7.317 s**,
existing Sleep false/true normal **2.17/2.2 s**; new table false/true race
**7.744/9.112 s**, existing Sleep false/true race **3.79/3.84 s**.
This is 20 test/subtest pass records (each new-table invocation includes its
parent and three scenarios), zero failures, zero skips, no race report.
These final runs include the one-time forwarding fault guard; subsequent
matching enqueue calls would forward unchanged rather than suppress replies again.

During this delivery, all eight normal/race and queue-mode selections were
independently invoked again after gofmt. Filtered JSON test events confirmed the
same 20 pass records and zero skips/failures; no race detector failure occurred.
Those repeat outputs were not saved as additional local logs.

Local evidence directory: `/home/ashutosh/.cache/native-step-replay/`.

- `final-new-false-normal.jsonl`: SHA-256 `9ea6df0793e4f6cddd510fc8f98b709ae06cf0f5a40f14c3102fcca6d1298989`.
- `final-new-false-race.jsonl`: SHA-256 `f34a446fea0c1482aa1130fa26d81ff0541f9bffbcec2ada6e09286c37c68839`.
- `final-existing-false-normal.jsonl`: SHA-256 `7363dc0bbbc0e66d2ec5b091f74a84e45f5e68beb3f5bc330240f503824a7094`.
- `final-existing-false-race.jsonl`: SHA-256 `12ef8cc6079d7cbe7be6bfa3e75faed221fa68a0d817726f56b5466db8416a61`.
- `final-new-true-normal.jsonl`: SHA-256 `8c9f026a04c94ca7cb920472a745891395a717301ae1a2897f4c378f42553ccc`.
- `final-new-true-race.jsonl`: SHA-256 `920f02752a75876a32bf2584f26a799b298e7d3b23815ccbb3677ea63ded8196`.
- `final-existing-true-normal.jsonl`: SHA-256 `a7511ffbb0174d6e25253e13db26ebfc4e31ae8f61d0ca728cd87b9aa12cc2d3`.
- `final-existing-true-race.jsonl`: SHA-256 `f7e81c07487dbc5213d0bbac02a0152eefd51ec69d2c139d5073286f9228be06`.

`validation-summary.json` contains the per-command counts/times/hashes;
`evidence-sha256.txt` retains passing and earlier failed local log hashes. Logs
are local evidence, not committed portable fixture artifacts.

Earlier failed logs are retained, with no production edits or runtime expected
fixture generation: `initial-false.log` used one worker below the scanner's
minimum-free-worker threshold; `repair-false-01.log` omitted the SDK-recognition
header; `-02` expected boolean text for the negotiated `die` field; `-03` assumed
no executor HTTP timing metadata; `-04` used Schedule's zero returned metrics
instead of the independent event-state size; `-05` compared function pointer to
value; `-06` reused retry cursor 0 for discovery (actual cursor 1). `-07` passes
before final header/index refinements. Initial `new-true-normal.jsonl` omitted
the distinct `:start` backlog in the raw-read recipe; final `-02` corrects this
using the owner-defined key, without weakening exact membership or score checks.

`git diff --check` and local Markdown target checks pass. The existing
`scripts/check-plan-status.sh` still exits 1 solely for unchanged
`docs/plans/run-ai-debugging.md`, status `Implemented — pending staging review`;
see `plan-status.log`. No new plan-status error was introduced.

Final test-source SHA-256:
`recovery_native_step_test.go` = `39718b6fac895be8eda36e9ff9c5f9e82457f2d18eadb2604f6e819ad9eec59f`;
shared `recovery_replay_test.go` = `d3d4e28df0158a682635c0995d305317625792b770dcbda07de50d6f0c88f37d`.
Fixture SHA-256: `native-boundary.json` =
`263f3217fd93c94ade7aaaf663c1842be9baa06f776aa8bd075c85228e15cd7f`;
`native-checkpoint.http` = `9ea611de08200961f0407b2aacabeb6bd9fc360133c204d908ccd3adca43de65`;
`native-terminal.http` = `d65f3eae9a7d2d1fe68e75c838b9405e457397346956813c9c0a1adc4b55b011`.

## Delivery metadata

[PR #16](https://github.com/ashutoshpw/inngest/pull/16),
`test(executor): verify fresh-owner native step replay`, targets
`t3/native-step-persistence-baseline` (PR #15). It opened at
`86a4bb1ad8e02859d1215a8a7a87b878e76f4d80`, directly based on
`6dcd3b71fa4ba5dcaa98c1562c5f072ecc30d6ee`. The opening snapshot was open,
non-draft, merge state clean, with no review decision or check runs. T3 linked
PR #16 to this child thread and parent thread
`c37dccc0-74a6-4b56-bc19-0ffcf321cc19`. The metadata follow-up advances the PR
head; the next isolated Rust adapter will use PR #16's final head as its base.
No merge, watch, force push, or polling was performed.

Independent Luna review found no blockers and the parent accepted this bounded
Go-only baseline. No production Go/Lua/Rust or module file changed. Actual native
adapter/runtime tests, release qualification, broader recovery and the stated
license gate remain open.
