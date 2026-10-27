# Bounded authorization state repair (2026-10-27)

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
Full-grid reconstruction refuses the partial. Pilot05 is EXHAUSTED, memory
INCONCLUSIVE, independent result review pending with parent. No retry, repair,
performance conclusion, delegation/commit/push/PR/campaign or broader gate change.
See [pilot05 report and exact evidence](response-owner-capability-pilot-05.md).
All earlier current-state/pending-review claims below retain historical scopes.


## Resource contract amendment — 2026-10-27

Parent explicitly accepts the resource-contract amendment after the pilot04
audit. The [current resource contract](response-owner-resource-contract.md)
supersedes earlier conflicting resource/activation wording prospectively only.
Verified separate Valkey FD enumeration is optional diagnostic evidence;
required owner resources, authenticated INFO memory and verified owned-store
identity remain strict. Unknown descendants stop without owner attribution.
Pilot04 remains a valid old-contract failure and EXHAUSTED; no new grant,
retry, capture, privilege or policy change. Implementation and the single
no-process fixture package await independent review. Earlier sections, including
former “current” states, null-registry/build/no-Go-change statements and
store-FD-denial rules, describe their original historical snapshots.

Implemented, uncommitted; **prior reviews accepted, authorization none, capture
closed**. Both zero-attempt pilot01/pilot02 authorizations remain exhausted.
A fresh readiness review of this repair and parent acceptance are required
before any new pilot authorization. This task issues no grant and runs no pilot,
owner workload, Docker, benchmark, delegation, commit or PR.

Workspace: `/home/ashutosh/.t3/worktrees/inngest/t3-7411bca6`, branch
`t3/readiness-comparator-harness`, base/HEAD
`e6c3f82c0146ed37ef626d739c75fc85057885b7`. The parent accepts
readinessreview03, clockaccountingreview02, P3precedencereview01,
runnerstopreview01 controls and declarationreview02. Their stale pending-review
markers were status bugs, not semantic mismatches. Earlier reports preserve
historical scopes; this record supersedes their current-state descriptions.

## Contract and complete gate inventory

This is a local test-harness contract, not a general authorization service. A
source-owned JSON record is read by Python and embedded in Go. Arbitrary runtime
JSON, environment booleans, the old fixed authorization phrase and external
claims cannot supply review acceptance or register a grant. Source modification
and fresh builds require the parent's explicit future activation instruction.
The host/source checkout and parent-owned claim directory are trusted; markers
are not protection against an operator deleting files or modifying source.

1. `pkg/execution/executor/testdata/response-oneshot-authorization.json`:
   schema 1, one exact recognized prior-review acceptance identity, and
   `authorization: null`. A future grant is a complete seven-field object:
   `id`, `scope`, `declaration_sha256`, `out`, `claim`, `expires_unix`,
   `attempts`. Scope is exactly `oneshot-capability-60-plus-8`; attempts exactly
   one; expiry is an integer Unix second; output and claim are absolute paths;
   claim must be outside output. ID matches `[a-z0-9-]{16,80}`. Declaration SHA
   covers the exact literal bytes, including the unchanged numeric budgets/grid.
   Unknown/missing/duplicate fields and malformed types fail closed. No grant
   has been installed. Known review acceptance is independent of grant state.
2. `scripts/response-owner-oneshot.py`: `authorization_state`,
   `strict_authorization`, `validate_authorization`,
   `check_capture_authorization`, `claim_authorization`, `capture`,
   `capture_bundle`, `main`. Both callable capture routes validate the
   source-owned record before host/source inspection, output creation, store or
   children. The inner route atomically creates and fsyncs the exclusive claim
   before capture effects; even a subsequent scope/provenance stop exhausts it.
   An existing claim **or** runner marker refuses entry. A racing second caller
   loses exclusive creation. The claim is never deleted, reset or retried.
   After provenance checks, expiry/scope is checked again before store intent.
   The original 180-second package and 6+4-second reserve remain unchanged.
3. `validate_build_contract`, `build_configuration`, `execution_environment`,
   `source_manifest`, `command`, `verify` inside `capture_bundle`: unchanged
   toolchain/optimization/source/binary/argv/environment/external-Cargo-config
   checks. There is **no separate capture-eligible receipt boolean** in this
   implementation. The current source-owned grant, declaration, scripts and Go
   embeds are included in source provenance. Old receipts cannot validate new
   source. Linux amd64 qualification, external output, capacities and fixed
   runner argv/timeouts remain. Failure consumes the claimed attempt.
4. `store_intent`, `prepare_store`, `docker_command`, `verify_store_identity`,
   `cleanup_store`: unchanged parent-owned unique UUID/name/label/full-ID store
   contract, cached image/no pull, bounded Docker calls, identity-checked removal
   and confirmed absence. `Budget`, `monitor`, `terminate`, `Stream`, `write`
   retain absolute deadlines, group absence/reaping, bounded logs/resources and
   sealing. Cleanup uncertainty is never normalized into success.
5. Go `TestResponseOneShotPilot`, `oneshotAdmission`, `oneshotDecodeStrict`,
   `oneshotClaimRunner` in `response_oneshot_readiness_test.go`: the same embedded
   review/grant record is required. No environment token can enable a null grant.
   The exact persisted Python claim must match the embedded grant; an exclusive,
   fsynced `.runner` marker prevents direct-entry replay. This happens before
   store attachment. The complete existing fixed-declaration validator and
   0 < remaining_ms <= 170000 check still precede store attachment. Expiry is
   rechecked before attachment and before every slot's setup/dispatch. Missing,
   wrong, expired or exhausted permission cannot reach store/owner dispatch.
6. Go `oneshotAttachStore`, `oneshotPreload`, `oneshotSetup`, `oneshotInvoke`,
   `oneshotCommand`, `oneshotDispatch`, `oneshotValidateInvocation`: accepted
   store/source/Lua/identity/observer/disposition/accounting/error/cleanup controls
   remain. Dispatch now also rejects any result blocker before the next slot,
   retaining raw invocation/readback/failure evidence. No blocker is cleared.
7. Go `oneshotReadback`, `oneshotDurableShape`, `oneshotNormalizeSnapshot`:
   durable raw bytes/hash are still sealed before validation. The unconditional
   stale pending-review append is removed; all prior item/payload/group/clock/
   FIFO/index/key/TTL/numeric assertions and normalization are unchanged.
   Existing result blockers survive verbatim. Acceptance is admission state,
   never an invented durable mismatch. This validator path is explicitly part
   of the repair and future activation; no further review-marker switch remains.
8. Python `reconstruct`, `validate_durable`: capture passes the exact grant and
   verifies it against the source record, sealed claim and runner marker before
   reducing rows. Posthoc expiry uses historical admission (not present wall
   time), so expiry cannot erase an already admitted result. All raw identity,
   phase/order, outcome, numeric, durable/hash/provenance, cleanup and blocker
   rejection remains; a grant cannot remove any such blocker. Reconstruction is
   read-only and cannot admit a capture. Fixture/CLI `reduce` without a grant is
   explicitly returned with `capture_authorized: false`; capture reconstruction
   validates its receipt and returns true. Selection remains supplied by the
   fixed capture entry; small synthetic selections remain available to functional
   fixtures and do not dispatch anything. Resource summary/memory conclusions
   remain INCONCLUSIVE and thresholds remain unapplied.
9. Declaration `response-oneshot-declaration.json`: only stale `status` and
   `blockers` metadata changed to `REVIEWS_ACCEPTED` and `[]`. All 68 rows,
   seeds/payloads/phases/order, 60 observations/eight controls and 180/6/4 bounds
   are byte-for-value unchanged. Go exact-declaration admission remains strict.
   Go child/native descriptor entry, ordinary functional/protocol/clock/error/
   refused-connect tests and `TestResponseOneShotOwnedStoreControl` retain their
   separate functional contracts; they are not capture admission routes. They
   were not selected here. Comparator, owners and helper lifecycle stay frozen.

A complete read of these entry-to-receipt paths plus the review/authorization/
blocker/eligibility search found no other executable pending-review hardstop.
That guarantee concerns this harness's review-state gates; genuine readiness,
provenance, platform, resource, cleanup and semantic failures still stop work.

## Complete future activation, requiring parent authorization

1. Review this entire state change, including Go admission, validator append
   removal, immediate result-blocker dispatch stop and Python reconstruction;
   record the fresh readiness review and obtain parent acceptance. Prior accepted
   reviews are retained; this repair does not claim independent acceptance.
2. Parent explicitly issues **one new** bounded grant after that review. Supply
   a new ID, exact external output, stable external claim path with an existing
   parent directory, integer expiry, fixed scope, attempts=1 and current literal
   declaration SHA. Neither exhausted pilot grant may be reused. Authorize
   replacing only `authorization: null` in the shared source registry with those
   exact values. No additional Go/Python entry/validator/receipt switches are
   necessary or allowed by this activation procedure.
3. Rebuild/reseal provenance **after** the registry edit: Go normal and race
   `native_step_integration` binaries (the record is embedded), native release
   helper and private native test artifact using the four existing exact build
   commands/profiles accepted by `validate_build_contract`. Record actual
   argv/cwd/sanitized environment/exits, all source hashes/code digest, tools and
   binary hashes. A receipt-only boolean is insufficient. Reuse the accepted
   clock/comparator/runner-stop/declaration controls; change no algorithm,
   thresholds, lane selection or numeric budget. Changed source cannot use the
   old four-role receipt. Complete bounded readiness checks before capture.
4. Under that separate instruction only, use the existing capture CLI with
   `--parent-authorization <new-id>` and the exact authorized output/artifacts/
   build receipt/Go path. Python claims the attempt, checks provenance, owns the
   store, passes ID and fixed selection/remaining budget/rows/store paths into
   Go; Go verifies the exact claim and claims the runner before store attachment,
   then performs per-slot invocation and durable validation. Reconstruction
   verifies authorization evidence plus all real result blockers. No review
   marker can be appended after admission; no result blocker can be bypassed.
5. Any claim, scope stop, failed build/provenance/capture/validation/cleanup after
   claim consumes the attempt. No retry/replacement/deadline extension. Retain
   claim and runner markers and raw failure evidence. Later authorization needs
   another explicit parent instruction/new ID/path and fresh provenance; retire
   the source grant to null only as separately directed. Historical reconstruction
   uses its retained source/registry/receipt snapshot, not an invented claim.

This task does none of these activation steps. Current registry remains null.

## Functional test ownership and evidence

Authoring gate: Python owns actual callable admission/claim/reconstruction
behavior; earlier tests covered only permanent denial, not accepted future
states. Go independently owns embedded-state transport, strict JSON/claim replay
and the durable normalizer that Python cannot execute. Existing dispatch controls
now include a genuine result blocker. Credible regressions are stale review
appends, token-only admission, ignored scope/expiry/attempts, replay, removed
result-blocker stopping and normalization of real failures. No production seam
or owner algorithm was added; all helpers belong to the existing test harness.

Seven selected Python methods use synthetic sealed claims/readbacks, invalid
scope/expiry controls and actual callable entry rejection; no subprocess or
Docker is run by that selection. The existing build-provenance fixture stops at
a mocked provenance command and never dispatches. Fresh Go normal/race selections
cover authorization, durable queue fixture, exact fixed declaration and synthetic
dispatch controls; the dispatch controls launch only the test executable in
synthetic record mode. No Go/native owner, real readback, store or 60-observation
pilot runs. Two cache-only overlays prove expected failures: restoring the old
pending-review append fails the new durable fixture's empty-blocker assertion;
priority 20 instead of 19 still fails the actual numeric validator. These are
functional negative controls, never capture-build provenance.

Evidence directory:
`/home/ashutosh/.cache/oneshot-authorization-state-fix-20261027/`.
`validation-summary.json`, `build-receipt.json`, `source-delta.json`,
`source-after.json`, `history-validation.json`, command/log receipts and
`evidence-sha256.json` provide exact final hashes and counts. An intermediate
compile failure (an overbroad edit also touched the owned-store test) is retained;
that edit was corrected before final passing builds. Initial successful Python
pass output was superseded before unique receipt naming was introduced; final
receipts are explicit and no failure is hidden. This is focused verification,
not a full suite or new global capture receipt. No native build is needed for
unchanged native source in this repair; future capture requires all four roles.

Only five state-related executable/fixture source files differ from the entry
freeze: Python harness/tests, Go readiness test, declaration metadata and the
new shared authorization record. Existing dirty owner/clock/comparator/Rust/
helper edits are preserved exactly. The historical 732 files in ten directories,
plus pilot02's own 41 files (773 total), remain sealed and unchanged. Plan/status
and this reference/readiness reference are the only documentation changes.
The unrelated plan-status error stays untouched. Memory INCONCLUSIVE;
FFI_NO_GO/SCREEN_NO_GO and all broader rollout/platform/license gates remain.
