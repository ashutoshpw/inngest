# One-attempt native response capability pilot 04 (2026-10-27)

**EXHAUSTED after exactly one actual attempt; STOPPED on live Valkey FD denial.**
Parent accepts the FD repair and resourcebatchreview02 P2 closure, authorization
review and every prior scoped semantics/fairness/clock/P3/runner/declaration review.
This closes collector P2; independent review of this pilot04 result remains pending.
Pilot01/02 remain scope-closed with zero actual attempts. Pilot03 remains spent:
one attempt, five validated observations and one cancelled row; its original
exit mechanism is unproven. None of its sealed source/raw/receipts was altered.
Pilot04 remains the separate-store FD failure; pilot05's ambiguous owner teardown
sample does not change that classification. See the [consolidated cross-pilot
audit](response-owner-capability-pilot-05-audit.md).

Workspace `/home/ashutosh/.t3/worktrees/inngest/t3-7411bca6`, branch `t3/readiness-comparator-harness`, base/HEAD
`e6c3f82c0146ed37ef626d739c75fc85057885b7`. Everything remains uncommitted.
The exact parent instruction is retained in `/home/ashutosh/.cache/oneshot-capability-pilot-04-20261027/parent-instruction.txt`.
No delegation, commit, push, PR, campaign or production action.

## Grant and activation

Only the registry authorization object was replaced with the separately authorized
new grant `rust-native-oneshot-pilot-04`. Scope is `oneshot-capability-60-plus-8`,
attempts **1**, integer expiry **1793119846** (one hour at installation).
Literal declaration SHA-256:
`51f2d88d4d011eeba69de1628f028ffa37413d110aa8773883ddd319c40aa033`.
Fresh exact output: `/home/ashutosh/.cache/oneshot-capability-pilot-04-20261027/capture`.
Fresh exact claim: `/home/ashutosh/.cache/oneshot-capability-pilot-04-20261027/rust-native-oneshot-pilot-04.claim`;
the Go runner additionally created `.claim.runner`. All owned output, store
receipts, logs and cleanup evidence are bounded under this new parent directory.
No prior claim/path was reused. The source registry retains the exact pilot04
grant, both permanent markers remain, and read-only admission now refuses with
`capture authorization exhausted`. No grant retirement, retry or successor grant.

Before source change, five state-applicable Python methods and six deterministic
collector methods (including seven synthetic finalization cases) passed. Both
public capture entries rejected spent pilot03. Null-specific tests were unsuitable
and were not selected. These checks launched no actual children or stores.
After grant, all four exact accepted builds succeeded: Go 1.26.4 normal/race
`native_step_integration`, Rust 1.94.0 release helper and private debug test
artifact (`--no-run`, not a native test execution). Sanitized environments,
actual structured argv/cwd/exits/logs, tool/compiler hashes and configuration
are retained. Native builds reused matching cached targets; their outputs were
copied into this fresh evidence directory and hashed. No algorithm edits.

Preflight, before claim, verified the full current source, all four binary and
command receipts, compiler hashes/versions, exact accepted optimization/config,
declaration/grid, fresh output/claim/runner-marker absence, path scope and
expiry, cached Valkey image and existing Docker access. Normal/race selected
fixed-declaration and accepted-review durable fixtures each passed **20 records**;
no owner/store was launched by them. Capture independently repeated provenance
inside its timed package. No public guard, privilege, policy, sampling interval,
threshold, numeric profile or lane selection changed.

## Actual capture and first blocker

The existing CLI ran exactly once, from **2026-10-27T15:51:40.918467+00:00** to
**2026-10-27T15:52:02.114765+00:00**, exit **1**. External monotonic elapsed time, including the
existing sudo same-user/docker-group/env/Python wrapper, was
**21.196297150 seconds**. Manifest start/end interval:
**21.020590104 seconds**; runner receipt interval:
**1.314409086 seconds**. The full CLI interval bounds
capture entry, provenance/archive, store setup, work/readback, stop, cleanup and
sealing below the unchanged **180-second cap**, with **6+4 seconds** reserved.
The internal Budget epoch is not separately emitted. Builds/preflight were outside
capture; store creation was inside. No deadline extension or reserve change.

First blocker, retained verbatim in the final STOPPED manifest:

```text
ProcSampleError: STOP: resource sample live_or_ambiguous_field_loss
```

The raw failed resource sample is **Valkey PID 1554902/start ticks 3129131085**,
UID/GID **999/999**, state **S before and after**, matching identity, FD-directory
ownership 999/999, and EACCES **13** at `/proc/1554902/fd`. Raw stat/status,
credentials, FD error/type/full filename, field timestamps and observed RSS/HWM
13512 KiB remain in `manifest.json.failed_resource_samples`; FD count is **null**.
No exit is established, so the accepted same-identity proven-exit exception does
not apply. This is a real live-denial blocker under the accepted collector,
including its separate-store role. Sparse sampling alone did not fail capture.
No privilege/policy change or broad permission exemption was attempted.

The supervisor sent SIGTERM during slot 5. That Go owner child records
`signal: terminated`, exit -1, empty stdout/typed outcome and confirmed cleanup.
Its raw durable snapshot was sealed before validation then failed on the empty
save outcome with `unexpected end of JSON input`. It is not an accepted row or
an established owner semantic mismatch. No slot 6 dispatch occurred.

Actual immutable counts:

- **1 claim, 1 runner admission, 1 actual capture attempt**.
- **6 starts/owner children** (3 native, 3 Go), all first starts.
- **5 validated observations** (3 native, 2 Go), **1 terminated Go row**.
- **0 repeated starts, 0 controls**, 62 declaration rows unstarted.
- **12 row events** (6 invocation, 5 validated, 1 failure), **6 start records**,
  **6 raw durable sidecars**, including the failed row.
- **0 retries/replacements/extra uncertainty experiments/in-process owner calls**.

The fixed contract remains 60 observations: 3 seeds × 2 payloads (64/65536) ×
2 lanes × 5 starts, plus 8 controls, with exact deterministic paired order,
independent durable initial state, authenticated Valkey fixed queue/layout and
fresh per-child clients. Only the first six declared rows started.

## Reconstruction, resources and cleanup

Full-selection Python reconstruction refuses with `ValueError: unknown row phase`
because the failure event is preserved. No shortened selection was substituted.
`raw-descriptive-intervals.json` retains all six completed/interrupted rows'
boundaries, outcome, usage and cancellation status without a fixed-grid result.
The actual Python durable validator independently verifies the five accepted raw
readbacks; failed-row envelope/hash/selection/child bindings also verify without
asserting semantic acceptance. Native success saves report one attempt and
Confirmed save/enqueue; no unrun pending/reject/savefail/enqueuefail conclusion.

The 17 resource stream events observe usable RSS for only one owner lifetime
(one sample on the interrupted Go child); five lifetimes, including all three
native children, have zero usable RSS samples. This is missing coverage, never
zero memory. Current parent/runner maximum stream gaps are
**691.100599/691.146856 ms**. Historical pilot03 gaps remain
**699.441543/699.460719 ms** and its original exit cause stays unproven.
There are no established-exit stream samples in this attempt. Valkey partial
resources are separate and never added to owner RSS. Wait Maxrss remains raw,
can include fork memory and is not native heap. Memory is **INCONCLUSIVE**.

One uniquely owned authenticated Valkey 8.0.3 store was created. Full ID:
`b8cd3f38cc2558bd8cd511243fd07df083e54622cf4a63f295be13181880e4b4`;
name `oneshot-75437c36dee742dbb70dddfc1d3252e4`, matching unique owner label.
Seven parent Docker command receipts retain create/ownership/start and absence
checks. Runner group absence is confirmed; exact store ID/name/label cleanup is
**CONFIRMED_ABSENT** in **0.064326392 seconds**.
All **62 pre-existing containers** retain IDs, names, images, status, running
flag, PID, start/finish times and restart counts. Autonomous health logs and
credential-bearing arbitrary configuration were not collected or compared.

## Provenance and preservation

All **13,418 captured source entries**, including symlink target contents, match
the immutable archive; all **29 capture artifact seals** verify. All **1,112
historical files** across sixteen directories remain unchanged: the original
982-file history (including pilot03's 30 capture files), 28-file collector repair
cache and 102-file P2 repair cache. Prior ledgers and original claims verify.

- Normal Go: `98597065ceba39d6704773371893231ff721e1cab4a5995cf91017dfbaff089e`.
- Race Go: `ef9db9d21f9e793c1fc3b0090747bba1e6fc4296a4cb2355ce192e1c076212ba`.
- Native helper: `da132d4a292fd6a3645ec538d0971bba5b25dd04440ab72cd576cb9eea57126c`.
- Private native tests: `c3c2ea1731a45746d0918acd096e442a626f5e4112899b0ffb231274da6f409f`.
- Four-role build receipt: `9ae4410bc205e80207a28e28b21e837f53d2bdfbbe14742183ab04e02af00a99`.
- Captured source code digest: `b859e778727046c972e9a41a2ca382af322928eedf5ded18cea99cb8953b5ae2`.
- Source archive: `92510465d21e047997830e7f407a3d5d4c0b31221f8f0475ab23f5063bd543c7`.
- Capture manifest: `3b636f28d37ee8d37e1230538fa5d8577bd38056bcaa954382e609bd3a84744f`.

Evidence: `/home/ashutosh/.cache/oneshot-capability-pilot-04-20261027/`. Key receipts are `validation-summary.json`,
`build-receipt.json`, `preflight.json`, `prior03-exhausted.json`,
`lifecycle-02-exhausted.json`, `failed-resource-classification.json`,
`raw-reconstruction.json`, `raw-descriptive-intervals.json`,
`resource-coverage.json`, `source-archive-verification.json`,
`history-validation.json` and `unrelated-container-verification.json`.
Raw capture is its `capture/` child; the final source delta and hash ledger are
separate from the immutable capture. This report/collector acceptance and final
plan/status/readiness prose follow capture and do not claim fresh compiled
provenance for post-outcome documentation. The registry and all executable,
collector, test, declaration, owner, fixture and helper bytes stay fixed after
capture. No development failure, partial row or resource record was removed.

Capability completion is **not achieved**. This is retained partial capability
and descriptive raw evidence only: no tail/p95 gate, language advantage,
production-Go, steady-state, transport or memory-PASS claim. FFI_NO_GO and
SCREEN_NO_GO, Phase 1 partial, Phase 2 open, bridge unset and all activation,
licensing (repository SSPL/workspace Apache), arm64/musl/final-link, macOS/Windows/
cloud, observer/lifecycle, fencing, crash/lease recovery, rollout and rollback
gates remain unchanged. Independent pilot04 result review is pending with the
parent. Work stops after evidence verification/sealing; no further run.
