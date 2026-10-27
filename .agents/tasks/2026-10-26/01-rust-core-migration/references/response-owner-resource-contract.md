# Resource review P2 repair — 2026-10-27

## Current activation: pilot05 exhausted (2026-10-27)

Parent accepts scoped resourcecontractreview02 and all prior reviewed controls.
One fresh pilot05 registry grant, four exact accepted builds and no-owner
preflight preceded exactly one CLI invocation. It STOPPED on strict owner
FD EACCES13 plus missing RSS/HWM, with no established exit, in **21.612119956s**
under the unchanged 180/6/4 budget. Six actual children/validated first starts
(3 native/3 Go), one additional journaled pre-child stopped slot, zero repeated
starts/controls; 14 events, seven journal records, six durable sidecars retained.
Store and runner group CONFIRMED_ABSENT; 62 other container identities/lifecycles,
13,420 archive entries and 33 artifact seals verify. Original 1,331 history
entries and all later cache/ledger evidence are preserved, including the old
registry's exact bytes alongside its authorized prospective replacement.
Six invocation identities are fresh, but collector publisher samples cover only
slot5; **cross-row runtime publisher transition coverage is not established**.
Full-grid reconstruction refuses the partial. Pilot05 is EXHAUSTED and memory
INCONCLUSIVE. The parent accepted Sol's independent audit of the retained
evidence, without accepting the incomplete grid or publisher coverage. No retry,
repair, performance conclusion, delegation/commit/push/PR/campaign or broader
gate change. See the [pilot05 report](response-owner-capability-pilot-05.md)
and [consolidated audit](response-owner-capability-pilot-05-audit.md). The
resource-contract review acceptance is limited to its declared controls; all
later P1/P2 lifecycle findings remain open. Earlier pending-review language
below retains historical scope.


Implemented **uncommitted, pending independent parent review** in the assigned
`t3/readiness-comparator-harness` worktree. This section describes the current
source delta; all earlier sections below retain their historical evidence scopes.
No new authorization or capture eligibility is created. Pilot04 and its exact
registry/claim/runner marker remain spent; memory remains **INCONCLUSIVE**.

## Findings and fixes

1. **Stale spawn membership.** The old positive-only `unix_ns` check and cached
   PID binding admitted stale receipts. The supervisor now generates a fresh
   runner UUID generation only inside the already authorized capture path and
   records it in the runner command environment. The projection and synced
   pre-dispatch start journal carry the exact capture grant ID and generation.
   The collector checks the bounded journal (64 KiB, at most 68 rows) against the
   parent's fixed declaration prefix, actual Popen runner PID and parent-held
   grant/generation. The current selection comes from that checked prefix, never
   the spawn receipt's role label. Spawn selection and invocation epoch must match
   that active journal entry, alongside the existing exact parent/child/stat/PID,
   start-tick, PPID, lane and argv checks. The spawn timestamp must fall between
   the journal epoch and the collector's recorded observation, with the journal
   bounded below by the recorded runner launch start. These are actual launch /
   observation anchors, not an arbitrary age allowance or positive timestamp.
   Every descendant sample resolves its receipt again, including cached
   PID/start-tick matches and new invocation transitions. The sampled `/proc`
   identity must still match the receipt. Generation plus declared invocation
   and sampled identity provide local protocol evidence; this makes no global
   process identity or scheduling guarantee. The checkout, parent claim directory
   and runner journal retain the existing trusted-host assumption. Missing,
   partial, stale or racing publication fail-stops; no retry is introduced.
   Go still publishes immediately after Start and before Wait; only projection
   metadata and journal metadata change, with no owner/comparator/effect change.
2. **Capacity lost partial evidence.** Both capacity paths now use
   `ProcSampleError`, preserving the collected batch through the real monitor
   resource stream and final STOPPED manifest. The extra failure record has
   `unavailable: capacity_failure`, capacity name, strict limit and timestamps;
   it has no PID or fake metric. Cache insertion stops before exceeding 128;
   runner/descendant sampling stops before exceeding 32 processes (plus the
   supervisor). A failed batch can additionally contain the metadata record;
   it cannot become a successful resource summary. No cap or sampling threshold
   is enlarged. Overflow fixtures allocate only 34 proc directories (including the supervisor
   and runner) or 128
   cached bindings, and stop before reading the overflowing process.
3. **Stale Go null-registry expectation.** `TestResponseOneShotAuthorizationState`
   now parses an explicit isolated null fixture for the `none issued` contract.
   Separately, the embedded current registry is decoded and checked for its exact
   wrong-request and expiry rejection reasons. It does not read host claim files,
   use the wall clock, write the registry, claim the actual grant, or accept any
   error as sufficient. Existing synthetic parser/claim controls remain isolated
   in `t.TempDir`; public admission/claim guards are unchanged. Earlier reported
   null-test failures are historical; this task does not claim they were rerun.

Only `VerifiedStore` can relax FD enumeration. Authenticated INFO, identity and
all other required fields, strict owner FD failures and same-identity established
exit controls are unchanged. Partial batches, FD exit classification, all fixed
budgets, sampling interval/density thresholds and inconclusive memory semantics
remain unchanged. No Rust, comparator or effect algorithm change is included.

## Authoring gate and evidence

The collector fixture boundary remains the primary owner of attribution and
partial-failure preservation. New cases guard credible stale-generation,
same-generation old-invocation, cached-receipt and capacity regression paths
that earlier fixtures missed. They exercise actual `resources`, proc parsing,
monitor and final sealing with bounded kernel/filesystem fixtures. Finalization
coverage owns the distinct transport risk of losing a partial batch between
sampling and the manifest. No new production seam or real child is needed.
The null parser fixture guards null admission independently from a changing
embedded registry. Exact error assertions preserve the accepted denial contract.

Evidence: `/home/ashutosh/.cache/oneshot-resource-review-p2-20261027/`.

- Final bounded fixtures: **13 methods, zero failures/errors/skips**, in
  **0.239986974s**, with a 29-second alarm and subprocess-dispatch sentinels.
  Ten synthetic capture-finalization bundles retain matching stream/manifest
  batches and final artifact hashes, including both capacity overflows. A prior
  package passed the same 13 methods in 0.283254374s before the final cache-cap
  check and fixture assertions were tightened; it is not the final source proof.
- Entry collector: cached-receipt regression fails exactly because no structured
  rejection occurs; both overflow probes show the original plain RuntimeError
  without a retained batch. Generation probe adapts only the old function's
  call signature and supplies the unchanged declaration path: two intended
  failures show stale generation admission and continued cached old receipt.
  All harness-development logs remain. The first generation probe had an archived
  module-relative declaration-path error; the next had a secondary assertion
  context error. The corrected two-failure probe is the regression evidence.
- Current Go projection compiled with cached **Go 1.26.4**, local toolchain/cache,
  cache-local TMPDIR/GOTMPDIR, **GOMAXPROCS=2**, `-p=2`, normal and race. Final
  selection `^TestResponseOneShot(AuthorizationState|FixedDeclaration|AcceptedReviewDurableFixture)$`
  passes **30 test/subtest records per binary**, zero failures/skips/race reports.
  Exact argv/cwd/environment/times/exits/log hashes and scoped artifact hashes
  are in command receipts and `build-receipt.json`. Native artifacts are not rebuilt.
  This is compile and no-owner control proof, not runtime projection publication
  or four-role future-capture provenance.
- One initial Go normal selection mistakenly included `ClockStages`; it failed
  at the missing `NATIVE_CLOCK_TEST_BINARY` precondition before store or child
  dispatch. That log is retained, not counted as passing proof. The 17-child
  synthetic dispatch selection was inspected and excluded. Actual synthetic
  dispatch children, Go/native owners, stores, pilots and benchmarks: **zero**.
  Compiler/linker subprocesses and test binaries are build/test infrastructure.
- Both existing public Python capture entry guards reject a nonmatching request
  with the exact current-registry `expired or scope mismatch` reason, before
  artifacts or commands. Null-specific Python selections were not falsely rerun.
  Old historical guard failures remain preserved. The current registry and all
  sealed old cache evidence are checked byte-for-byte by `history-validation.json`.
  Fresh source-before/after hashes and patches are in `source-delta.json`; this
  prospective source/build delta never rewrites old build or pilot receipts.

The original resource-contract amendment and its evidence follow unchanged.
Parent owns independent review. No delegation, commit, PR, owner/store/pilot,
benchmark, new grant, privilege change or sampler performance project occurred.
Writes stop after the final evidence/preservation seal.

---

# Resource contract amendment after pilot04 audit

2026-10-27; implemented and **uncommitted, pending independent review** in
`/home/ashutosh/.t3/worktrees/inngest/t3-7411bca6`, branch
`t3/readiness-comparator-harness`. Parent explicitly accepts this prospective
contract amendment, not pilot04 rescue or a new execution grant. No delegation,
commit, PR, real child, store, pilot, retry, benchmark or build was performed.

## Required resources and diagnostic exception

Owner supervisor, Popen runner and actual Go/native owner identities require
PID + start ticks. Raw stat/status, RSS/HWM, threads and FD enumeration remain
required for live owners. Live owner permission failures remain fatal. The
accepted same-identity Z/X/x exit exception for FD EACCES/ENOENT/ESRCH is unchanged;
status errors, ambiguous disappearance, identity changes and other FD errors
retain their previous fatal classification. The proc sampler additionally checks
that both raw stat PIDs equal the requested PID. No unavailable metric becomes zero.

Only FD **enumeration** for a verified separately owned Valkey is optional.
`VerifiedStore` requires the parent's confirmed full container ID, UUID-derived
name and ownership label, bounded successful Docker inspection with running
state, image digest and host PID, matching before/after PID/start ticks and
Valkey comm, plus successful authenticated `INFO memory` for that exact container.
`used_memory` and `used_memory_rss` must be present numeric fields. The existing
fixed disposable-store password is used; no credentials or policy are changed.
The parent performs these two new bounded Docker commands only during a separately
authorized future capture, inside the unchanged package/reserves/command capacity.
Raw command bytes, errors and timestamps and binding-stat availability are sealed,
including partial binding failure. No such command was executed for this amendment.

Store stat/status, stable PID/start ticks, live state, RSS, HWM, threads,
container ID/name/owner label/image and authenticated INFO remain **required**.
FD-directory ownership metadata retains its pre-existing best-effort diagnostic
status; it was never a required field and is not an additional relaxation.
`store_resource` requires a validated `VerifiedStore` object and a matching runner
container/PID/image projection, then checks the expected process start ticks.
Only an otherwise complete live sample whose sole required-read error is FD
enumeration can continue. Its raw errno/error/path/availability/timestamps and
null FD count survive with `diagnostic_unavailable: ["fds"]`, never an invented
count. A role label such as `valkey_separate` passed to the strict proc sampler
cannot enable this exemption. Failed identity, absent INFO, another missing
required field or store exit still stops. Store resources and INFO stay separate
from owner RSS; summary FD peak stays null if every FD observation is unavailable.

## Ownership, discovery and publication race

Recursive thread/child enumeration is discovery only. Known membership is bound
to the supervisor identity, the actual Popen runner PID/start ticks, or the
runner's actual spawn projection: parent PID/raw stat, child PID/raw stat,
fixed declared selection/lane, exact expected Go/native argv and raw timestamp.
The Go change only projects these existing spawn facts after Start and before
Wait; it does not change comparator, owner, descriptor, clocks or store semantics.
Projection I/O is charged inside the existing outer boundary and its failure is
collector failure after normal Wait/cleanup. No Go compilation/execution is
claimed here; formatting and source inspection are static evidence only.

One atomic current-spawn file is bounded to 16 KiB at read. There is one sequential
owner at a time, and the existing 128-identity/33-process limits remain. Once a
projection is verified its identity is retained; each sampled owner record embeds
the receipt used. Before a streamed invocation receipt exists, this immediate
projection supplies runner identity evidence without relying on ancestry alone.
If discovery beats projection publication, or the projection is stale, malformed,
foreign or mismatched, the collector performs one read and **fail-stops** with
`unbound_descendant`. It does not wait, retry, infer role, charge unknown RSS or
ignore the descendant. This conservative race can stop a future capture; no
successful runtime race handling or capture completion is claimed by fixtures.
No additional service roles are reviewed/admitted. Docker daemon/shim/service
association cannot enter owner totals. Raw failed batches reach the monitor stream
and final STOPPED manifest; final hashes describe actual partial bytes.

The resource summary now charges only explicitly accounted supervisor/runner/
owner records. Old streams lacking the new accounting evidence are historical;
do not rerun the amended summary on them to reclassify their historical totals.
The sibling estimator fixture supplies the new explicit accounting marker while
retaining its independent literal PID-lifetime/peak expectations.

## Fixture authoring gate and one package

The collector is the primary owner of identity attribution, strict live denial,
verified-store diagnostic FD loss and partial-failure sealing. Existing proc
fixtures did not exercise the owner-membership or owned-store exception contract.
Credible regressions are ancestry-only owner charging, a role-label permission
bypass, PID reuse, missing INFO accepted as valid, dropping denial evidence and
sealing a partial batch as complete. Tests feed filesystem/kernel-read fixtures
and protocol receipts through actual collector functions. No test-only production
flag/export/seam was added. Existing FD-exit controls are retained, not duplicated
at an owner/comparator layer. The Go projection remains unbuilt pending future
separate authorization; fixtures test the consumed receipt protocol, not actual
Go publication or child startup.

Exactly one package command ran, with a 29-second alarm and subprocess-dispatch
sentinels; **0.198744142 seconds**, under 30 seconds. It first ran the single
unreceipted-descendant regression against archived entry source: one intended
failure, `AssertionError: ProcSampleError not raised`, zero errors. The same
regression then passes on amended source. The final selection passed **11 methods,
29 unittest subtests, zero failures/errors/skips**. These are fixture counts, not
owner executions or pilot observations. No retry or second fixture command ran.

The ten collector methods are:

- `test_unreceipted_descendant_is_fatal`
- `test_spawn_bound_owner_accounting_and_unknown_publication_race`
- `test_verified_store_fd_denial_retains_info_and_null_diagnostic`
- `test_store_required_info_identity_and_other_fields_stay_fatal`
- `test_partial_fields_survive_live_and_ambiguous_fd_loss`
- `test_only_confirmed_same_identity_exit_can_tolerate_fd_loss`
- `test_pid_reuse_or_unverified_exit_never_exposes_metrics`
- `test_status_oserror_is_fatal_even_with_confirmed_exit`
- `test_resources_keeps_batch_and_prior_pid_identity_on_mismatch`
- `test_resource_batch_reaches_monitor_and_final_capture_manifest`

The eleventh is existing
`Readiness.test_fixed_resource_estimator_preserves_pid_lifetimes`. The finalization
method retains eight synthetic bundles: unknown publication race; live owner FD
EACCES; task/children traversal EACCES, EIO and ENOENT. Every final resource batch,
STOPPED manifest and artifact hash is checked. The fake Popen operation is not a
real process. Actual children/stores/captures/pilots/benchmark invocations: **zero**.

Exact command, from the assigned worktree:

```sh
env PYTHONDONTWRITEBYTECODE=1 python3 /home/ashutosh/.cache/oneshot-resource-contract-20261027/fixture-package.py
```

AST syntax, Go formatting and whitespace/source-delta checks are static only.
No full suite, build, owner, store, sampler or real-child control ran. Earlier
collector-control packages remain historical and were not rerun.

## Evidence, preservation and remaining decision

Evidence is isolated at
`/home/ashutosh/.cache/oneshot-resource-contract-20261027/`: fixture runner and
receipt, final/baseline logs, eight `synthetic-final-artifacts/` bundles, entry
source snapshots, source-before/after/delta and patches, static verification,
history/pilot04 preservation manifests and final evidence hash ledger.
All **1,112** historical files and all **125** files in the pilot04 evidence
root, including its **30 capture files / 29 artifact seals**, remain byte-identical.
No historical claim, registry, capture, source archive, failure, report or receipt
is rewritten. All pre-existing source edits outside the declared amendment delta
remain unchanged. The pilot04 report itself is unchanged. Source delta is separate
from historical build provenance; the four-role receipt remains historical and
no four-role build is needed until a future separately authorized capture.

Current executable/test source SHA-256:

- Collector: `d2d978220a578b3822a97d9871e01821e69c92ce5d5280d9f900fb843530abb6`.
- Collector fixtures: `a8308e1f8faffc6c37a4743ceb1df6ff598b70a789ea0ea70d4092aad9a53266`.
- Sibling estimator fixture: `58ccde2302716a0556c50157916f2375716dfa95e1fd82e5aa9102d6bf333410`.
- Go spawn projection: `7452da9f09b5bc4a4d781db4b9f6b7c27201f811b06c8a41f1f15b15427f2f61`.

Pilot04 stays **EXHAUSTED**, one actual old-contract failure, five validated
first starts and one terminated row. It cannot be rescued/reclassified by this
prospective amendment. Pilot03 remains spent with its original exit cause
unproven. Scheduling, 5 ms requested interval, density threshold, exact declaration,
180/6/4-second budget/reserves, per-slot controls and authorization are unchanged.
The 691.100599/691.146856 ms pilot04 and 699.441543/699.460719 ms pilot03 gaps
remain; no sampler/performance repair or lower density threshold was attempted.
Memory stays **INCONCLUSIVE**, with no PASS, gate, production/language advantage
or performance claim. All prior no-go/platform/license/activation gates remain.

Parent owns independent review of this uncommitted implementation and evidence.
No review was delegated or claimed complete. Future capture needs a separate new
grant and fresh accepted provenance; this amendment creates neither. Writes stop
after final preservation verification and evidence seal.
