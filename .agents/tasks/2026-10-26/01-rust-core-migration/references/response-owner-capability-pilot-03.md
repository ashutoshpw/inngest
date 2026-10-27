# One-attempt native response capability pilot 03 (2026-10-27)

Parent accepts authorizationreview01 and every prior scoped readiness, clock,
P3, runner-stop and declaration review. This instruction authorizes exactly one
new `rust-native-oneshot-pilot-03` attempt, using the existing capture CLI and
isolated `TestResponseOneShotPilot`; no algorithm or other guard changes.
Pilot01 and pilot02 remain exhausted with zero actual attempts.

Pilot03 remains spent and its original exit cause remains unproven. The later
pilot05 owner teardown sample is a separate classification and does not
retroactively explain pilot03. See the [consolidated cross-pilot audit](response-owner-capability-pilot-05-audit.md).

Workspace `/home/ashutosh/.t3/worktrees/inngest/t3-7411bca6`, branch
`t3/readiness-comparator-harness`, base/HEAD
`e6c3f82c0146ed37ef626d739c75fc85057885b7`. All work remains uncommitted.
The full parent instruction and independent command/source/history receipts are
in `/home/ashutosh/.cache/oneshot-capability-pilot-03-20261027/`.

## Installed grant and lifecycle

Only `authorization: null` in the source registry is replaced. Grant scope is
`oneshot-capability-60-plus-8`, attempts 1, expiry Unix **1793116313** (one hour
from installation, covering builds and one capture). Declaration literal SHA-256
is `51f2d88d4d011eeba69de1628f028ffa37413d110aa8773883ddd319c40aa033`.
Exact new output: `/home/ashutosh/.cache/rust-native-oneshot-pilot-03-capture-20261027`.
Exact exclusive claim: `/home/ashutosh/.cache/oneshot-capability-pilot-03-20261027/rust-native-oneshot-pilot-03.claim`;
Go additionally creates that path plus `.runner`. No prior claim/output is reused.
Any post-claim failure consumes this grant; no retry, replacement or extension.

Before installation, seven selected Python methods and normal/race Go
`TestResponseOneShotAuthorizationState` pass. Existing null assertions are not
selected after installation. An initial external bookkeeping script used the
wrong old receipt key and stopped before tests/grant/claim; its original script
is retained. This is not a capture attempt or a failed null assertion.

The captured source snapshot records the pre-capture state as installed and
unclaimed. **Final state: EXHAUSTED after exactly one actual attempt, stopped
by resource collection failure.** The source registry retains this exact grant
for historical reconstruction. Its exclusive claim and `.runner` marker remain;
read-only admission checking returns `capture authorization exhausted`. No
registry retirement or second grant was needed or performed. The captured
archive/manifest/receipts remain immutable. This outcome prose, plan/status and
readiness updates follow capture and have a separate source delta; they are not
represented as bytes in the captured source manifest or as a newly built state.

## Actual attempt and stop

The only CLI invocation ran **2026-10-27 14:53:41.227529 UTC to
14:54:02.675054 UTC**, exit 1, **21.447523276 seconds** of external monotonic
wall time including the sudo/env/Python wrapper. The manifest's narrower
start/end interval is **21.234710248 seconds**; the internal Budget epoch is
not separately emitted. The full CLI interval bounds capture entry, provenance,
archive, store setup, owner work, stop, readback, cleanup and sealing below the
unchanged 180-second cap. Stop/sealing reserves remain 6+4 seconds; neither
deadline nor reserves were extended. Runner receipt interval is 1.546011624
seconds, including termination/finalization; Go reports its test at 1.26 seconds.

First blocker, retained verbatim in the STOPPED manifest:

```text
PermissionError: [Errno 13] Permission denied: '/proc/1327265/fd'
```

The supervisor failed while collecting the sixth owner child's FD count and
sent SIGTERM. The runner's parent context was cancelled during slot 5. That
child exited 0 and its retained raw stdout contains a success-shaped outcome,
but the cancelled parent did not accept/decode it. The typed outcome remains
empty. Independent durable bytes were still sealed before validation encountered
`unexpected end of JSON input` while decoding that empty typed SaveStep result.
Neither the raw stdout nor the saved effects turn this cancelled row into an
accepted observation. The underlying permission failure is not independently
diagnosed here, and no owner semantic mismatch or native hang is established.
No next dispatch occurred. No harness, sampler or algorithm repair was made.

Actual counts, retaining every completed and interrupted row:

- One Python claim, one Go runner admission, one actual capture attempt.
- Six started observations / six owner children: three native and three Go.
- Five validated observations (native 3, Go 2); one failed/cancelled Go row.
- All six are first starts. Zero repeated starts and zero declared controls.
- IDs 0–5 match the exact declaration prefix. No slot 6 or later start exists;
  62 declared rows remain unstarted (54 observations and all eight controls).
- Twelve row events: six invocation, five validated and one failure; six start
  records and **six raw durable sidecars**, including the failed row's snapshot.
- Zero replacements, retries, extra uncertainty experiments or in-process owner
  calls. The fixed 68-row declaration is unchanged; completion is not claimed.

Full-selection reconstruction refuses with `ValueError: unknown row phase`
because the failure event is retained. No smaller selection was passed to the
reducer. Separate descriptive artifacts retain all six rows' raw boundaries,
intervals, outcome/cleanup/error/usage and cancellation status. Python's actual
durable validator verifies the five accepted readbacks; the sixth raw envelope,
hash and child/selection bindings verify without asserting semantic acceptance.

The 89 resource records contain usable RSS for only one of the six owner
lifetimes (35 samples for the interrupted Go child). All three native lifetimes
and the other two Go lifetimes have zero usable RSS samples. Per-child Wait
usage remains raw; Maxrss may include pre-exec memory and is not native heap.
Valkey samples/INFO memory remain separate. Resource collection itself stopped
the attempt; memory is **INCONCLUSIVE**, regardless of the descriptive RSS sum.
The three completed native saves retain one attempt and Confirmed save/enqueue
status each. Pending/reject/savefail/enqueuefail controls were not reached; no
retry/uncertainty behavior is inferred or extra experiment performed.

## Store ownership and preserved history

Exactly one authenticated Valkey 8.0.3 store was created inside the timed package,
with fixed queue mode/layout and fresh per-child clients. Parent Docker receipts
number seven. Full owned ID:
`68544337b3ba9a0cdbc6561db44789eb2c67f43eb0866f98909e2c0acd9ef8a7`;
name `oneshot-c2ed5d4fdd8042bca48bce0110d1e1fd`, matching unique owner label.
The runner stopped and its process group is confirmed absent. Parent cleanup
confirms the exact store's ID/name/label absence, **CONFIRMED_ABSENT**, in
0.069490149 seconds. Go's owned helper cleanup and the parent receipts are
preserved. All 62 pre-existing container IDs, names, images, status, running flag,
PID, start/finish times and restart counts match before/after. Autonomous health
logs and arbitrary configuration/credential fields were not collected.

All **855 historical files across 12 prior one-shot evidence directories** match
their entry hashes, including sealed pilot01/02 and authorization-state repair.
Their old outcomes and zero-attempt authorization closures remain unchanged.
All **13,415 captured source entries**, including symlink targets, match their
archived bytes; all 29 manifest artifact hashes verify. The failed row, runner
log, resource stream, claim markers and original seals are retained unchanged.

## Builds, checks and evidence

All four exact build commands ran after installation using Go 1.26.4 / Rust
1.94.0, accepted optimization profiles, sanitized environments and successful
exits. Cargo reused the current source-matching cached release/debug targets
(recorded 0.09/0.05 seconds); it did not perform a separate native test run.
Fresh complete source/binary receipts bind those outputs to this activation.
The old Go functional receipt was used only for pre-grant null checks.

- Normal Go: `0f00ee8bf496613ece241ef617889aaf496a5d8d3a32a1ca763bee4b43f5c1f2`.
- Race Go: `c84b9079996236bc9dfcc6a4dfe76ae31c7358f4741af869dabe63c546cdac7b`.
- Release native helper: `da132d4a292fd6a3645ec538d0971bba5b25dd04440ab72cd576cb9eea57126c`.
- Private debug native tests: `c3c2ea1731a45746d0918acd096e442a626f5e4112899b0ffb231274da6f409f`.
- Four-role build receipt: `2cda9312dbcb4f24a604c40467456687d1aeaf3adf5011fc708d9978722b9a65`.
- Captured source code digest: `0765fe1e4d70122526c2740b998c96becaf3fa1221667aa68ddbbf8fff6b8a14`.
- Source archive: `fbdfb7e47cc1c5d61cd0c1f2b777d91f5b2b67a48e535fd2e46df4db8a99a9fb`.
- Capture manifest: `1786a8914fdc6e737ac1a352a211032f2026cb1329209aed20fcc13ab9cbde2e`.

Before grant: seven Python methods pass, and Go authorization normal/race each
produce ten pass records. After grant: only fixed-declaration and accepted-review
durable-fixture tests run, normal/race each twenty pass records. No unchanged
null assertion ran post-grant. Preflight checked exact fresh paths, declaration,
all four role receipts/artifacts, tool versions/hashes, full current source,
cached image and Docker access before claiming; it created no workload store.
Capture independently repeated its own provenance inside the package.

Raw capture is in `/home/ashutosh/.cache/rust-native-oneshot-pilot-03-capture-20261027/`.
External evidence in `/home/ashutosh/.cache/oneshot-capability-pilot-03-20261027/`
includes `validation-summary.json`, `build-receipt.json`, command/log receipts,
`raw-reconstruction.json`, `raw-descriptive-intervals.json`,
`partial-resource-summary.json`, `resource-coverage.json`,
`native-control-disclosure.json`, source/archive/history/container checks,
`lifecycle-02-exhausted.json`, post-capture source delta and final hash ledger.
External verifier development also stopped on an initial regular-file-only
archive assumption (CLAUDE.md is a symlink), then an incorrect five-sidecar
assumption. Those verifier versions/errors are retained; corrected checks read
the same immutable capture and never invoked capture again.

## Fixed contract and interpretation

Exactly 60 observations (3 seeds × 2 payloads 64/65536 × 2 lanes × 5 starts) and
eight declared controls, in the unchanged deterministic paired order. First
starts remain separate from repeated process starts. Existing authenticated
local uniquely owned Valkey, fixed queue mode/layout, cold clients and native
uncertainty/retry controls are used. The entire capture package, including
provenance/setup/readback/cleanup/sealing, is capped at 180 seconds with 6+4
seconds reserved. Stop on the first real blocker before another dispatch.
Builds and pre-claim readiness checks are outside that package; store creation
is inside it. Only the existing four exact build profiles/toolchains are used.

Raw reconstruction is descriptive capability evidence only. Memory stays
INCONCLUSIVE; no tail/p95 gate, production Go, language advantage, steady-state,
memory PASS or transport conclusion. FFI_NO_GO/SCREEN_NO_GO, unchanged
uncalibrated thresholds, platform/licensing/lifecycle/recovery/rollout gates and
unset bridge remain. No delegation, commit, push, PR, campaign or production.
Parent owns independent result review later.
