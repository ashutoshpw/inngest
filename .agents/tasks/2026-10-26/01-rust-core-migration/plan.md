# Convert the Go Execution Core to Rust

**Date:** 2026-10-26

**Task:** 01-rust-core-migration

## Objective

Migrate the Inngest execution core from Go to Rust in behavior-preserving stages, retaining persisted-state and SDK replay compatibility. Completion means the covered runtime has no Go execution or queue algorithm owner.

## Decisions and scope

- Target the OSS/self-host runtime evidenced here. First bridge validation targets Linux amd64/arm64. macOS and Windows validation is deferred, not retired, and is required before broader release or Go retirement. Cloud parity is a separate gate.
- Preserve currently implemented SDK request, request-version, and replay behavior. The configured SDK pins do not confirm minimum supported SDK versions; see the [SDK and deployment matrix](references/sdk-deployment-matrix.md).
- Eventual Rust scope includes trigger/CEL and schedule decisions, run/step transitions, queue coordination, state adapters, and recovery. UI, SDK implementations, and management API rewrites remain out of scope; gateways may stay behind explicit ports.
- The bridge spike compares FFI and a local process; bridge choice remains unset until independent review, matched measurements, and platform gates are complete. Keep decision logic deterministic and I/O-free.
- **Sequencing amendment adopted 2026-10-26:** broad Phase 1 stays partial. The bounded Linux amd64/arm64 read-only bridge **entry gate is satisfied** by the accepted exact-slice Go oracle, recorded Go microbenchmarks, and bounded existing DB/Valkey/lifecycle evidence. Phase 2 is not done and no bridge is selected. Each later Rust migration needs its relevant additional-owner baseline first. Full rollout/retirement gates remain unchanged; see the [bounded bridge contract](references/opcode-bridge-contract.md).

**Native-owner amendment accepted 2026-10-27:** stop current FFI-boundary expansion after SCREEN_NO_GO, retaining all negative evidence. The Go-only ordinary StepRun accepted-response → durable checkpoint → discovery baseline is accepted as a bounded owner baseline. Fresh Go runtime replay is the next layer, followed by native dependency qualification before a Rust port. That baseline did not authorize a native Redis client or worker; the separately accepted, internal native SaveStep prototype below remains unconnected to production and does not select a bridge or complete a broader phase. Rust remains pinned to 1.94 with existing serde/json/xxhash dependencies; reconcile repository SSPL and workspace Apache licensing before distribution.

## Current delivery handoff (2026-10-27; scoped work only)

- [x] Preserve the accepted readiness/comparator and bounded native clock, accounting, and standalone-precedence repairs; record reviews only at their declared scopes.
- [x] Parent accepted Sol's independent audit of retained pilot05 evidence. Preserve six actual children/six validated first observations and all partial artifacts; distinguish review acceptance from author execution and capability completion.
- [x] Keep grants01/02 closed at zero attempts and grants03/04/05 spent after one attempt each. Existing Python/Go one-shot guards reject retained claim markers; no grant is reusable.
- [ ] Resolve the open P1/P2 sampling and lifecycle findings through an explicit resource architecture decision before considering another capture.

Pilot05 remains **INCOMPLETE/EXHAUSTED**; memory is **INCONCLUSIVE**, cross-row publisher transitions are **NOT ESTABLISHED**, and no performance, production, steady-state, language, tail, or transport verdict is accepted. See the [consolidated pilot05 audit](references/response-owner-capability-pilot-05-audit.md). This handoff does not authorize a new grant or implementation. The older sections below preserve their source-time review snapshots; any older pending-review wording is historical.

Delivery is open in [PR #22](https://github.com/ashutoshpw/inngest/pull/22), stacked on [PR #21](https://github.com/ashutoshpw/inngest/pull/21), with base `t3/native-response-owner`. T3 lists both layers in the linked stack. The PR opening snapshot at `e7adbb35dac64062e4de8aee84ae2611286ad41d` was open, non-draft, mergeable, with no review decision or check runs reported. No merge, watch, polling, or force push.

## Capability pilot 05 (2026-10-27; exhausted after one attempt)

- [x] Read applicable protocol/authorization/resource contract; record scoped resourcecontractreview02 and all prior review acceptance.
- [x] Preserve original1,331 history entries and later ledgers; applicable no-owner checks pass; install fresh pilot05 grant and build all four exact artifacts.
- [x] Preflight current source/binary/tool/config/environment/paths before claim; existing CLI runs once in 21.612119956s within unchanged180/6/4, stops on strict owner field loss.
- [x] Self-verify six actual child identities/six durable observations, one pre-child stopped slot, raw reconstruction refusal, cleanup/62 containers, archive/artifact/history seals; cross-row publisher sample transitions NOT established.
- [x] Retain complete partial evidence and exact spent grant/claim/runner marker; update outcome and separate post-capture documentation provenance.
- [x] Parent accepted Sol's independent audit of the retained pilot05 evidence; this is not full capability acceptance. No delegation or additional attempt authorized.

See [pilot05 report and evidence](references/response-owner-capability-pilot-05.md) and [consolidated independent audit](references/response-owner-capability-pilot-05-audit.md). Evidence `/home/ashutosh/.cache/oneshot-capability-pilot-05-20261027/`; memory INCONCLUSIVE, capability incomplete, all prior no-go/platform/license/activation gates unchanged. Only grant/docs changed, no capture logic or privilege/policy/sampling changes. Earlier sections retain historical scopes.

## Capability pilot 04 (2026-10-27; exhausted after one attempt)

- [x] Record parent acceptance of FD repair and resourcebatchreview02 P2 closure, and all prior scoped reviews; verify pilot03 remains exhausted and preserve all historical evidence.
- [x] Pass applicable spent-state/collector checks without null-specific assertions; install the exact fresh pilot04 registry grant only.
- [x] Rebuild all four accepted artifacts and seal current source/toolchain/config/environment receipts; preflight before claim.
- [x] Invoke the existing fixed 60-observation/eight-control CLI once: live Valkey FD denial stops after five validated observations and one terminated row in 21.196297150s; retain partials and exhaust grant.
- [x] Refuse partial full-grid reconstruction; retain descriptive raw evidence, verify cleanup/62 other containers/1,112 historical files/source hashes, update outcome report and seal.
- [ ] Parent-owned independent pilot04 result review; no delegation or additional run authorized.

See [pilot04 report and evidence](references/response-owner-capability-pilot-04.md). Evidence root `/home/ashutosh/.cache/oneshot-capability-pilot-04-20261027/`; exact output is its `capture/` child and claim is its `rust-native-oneshot-pilot-04.claim` file. No algorithm, privilege, policy, threshold, commit, push, PR or campaign change. Prior sections retain historical scopes.

## Collector-only repair (2026-10-27; no capture authorization)

- [x] Preserve pilot03 failure classification/history; retain partial proc fields before FD reads and fail-stop on live/ambiguous denial or identity loss.
- [x] Run focused Python fixtures/static checks and exactly one <=30s launcher with three sequential synthetic children; preserve every outcome.
- [x] Record source delta/current hashes separately from historical build receipts; keep pilot03 spent and memory INCONCLUSIVE (known 699ms gap).
- [x] Parent accepts FD repair and resourcebatchreview02 P2 closure; the separate pilot04 instruction authorizes one new attempt. Historical repair scope remains unchanged.

See [collector amendment and evidence](references/response-owner-collector-repair.md). No owner/store/capture/benchmark/root/policy/delegation/commit/PR work.

## Capability pilot 03 (2026-10-27; exhausted after one attempt)

- [x] Record parent acceptance of authorizationreview01/all prior scoped reviews; preserve exhausted zero-attempt pilot01/02 and historical evidence; pass null-specific checks before grant.
- [x] Install only the new source-owned grant with fresh exact external output/claim, current declaration SHA, one attempt and one-hour integer expiry.
- [x] Rebuild all four exact artifacts and validate complete source/build/path receipts before claim; no workload store before timed capture.
- [x] Run the existing CLI once; resource sampling permission failure stops slot 5, with five validated observations, one interrupted row and zero controls; seal raw/cleanup evidence and exhaust grant.
- [x] Refuse full-grid reconstruction of partial evidence, verify all source/artifact/history hashes, record separate post-capture documentation delta and final limits.
- [ ] Parent-owned independent result review; no second grant or retry is authorized.

See [pilot03 authorization and evidence](references/response-owner-capability-pilot-03.md). No algorithm/other guard changes, delegation, commit, push, PR, campaign or production. Older sections retain historical scopes.

## Bounded authorization state repair (2026-10-27; capture closed)

- [x] Audit every entry, validator, reconstruction and receipt gate; separate accepted reviews, single-use grant and genuine result blockers.
- [x] Implement the shared source-owned contract with authorization null; remove only stale review metadata/validator append and stop dispatch on real blockers.
- [x] Verify synthetic authorization/durable/reconstruction controls and fresh Go normal/race builds; preserve historical receipts and source freeze.
- [x] Fresh authorizationreview01 accepted by parent; new bounded pilot03 grant issued separately. Both prior zero-attempt grants remain exhausted.

Reviews accepted; authorization none; capture closed. Full gate inventory, functional evidence, exact source changes and complete future enable steps (including validator path): [authorization state repair](references/response-owner-authorization-state.md). No pilot/capture/benchmark/delegation/commit/PR is authorized by this task.

## Capability pilot 02 (2026-10-27; closed before capture)

- [x] Record parent acceptance of readinessreview03, clockaccountingreview02, P3precedencereview01, runnerstopreview01 controls and declarationreview02 closure; preserve closed pilot01.
- [x] Inspect unchanged capture paths under the new one-attempt authorization. STOP before capture: durable validator still appends an unconditional pending-review blocker, rejected only by final reconstruction. Retiring it requires an additional validator-path amendment beyond the permitted entry/receipt switches.
- [x] Preserve executable sources and all 732 files across ten prior oneshot evidence directories; five existing focused Python methods pass, zero children/builds/Docker/capture workload. Seal new scope-stop evidence and close authorization EXHAUSTED; no implicit retry.
- [ ] Parent-owned independent result review; no delegation or independent acceptance claimed here.

See [pilot02 scope-stop report](references/response-owner-capability-pilot-02-scope-stop.md). The 180-second capture package never began; old build receipts remain historical. All earlier checklist/review descriptions below retain their historical scope.

## Capture control implementation (2026-10-27; no capture authorization)

- [x] Enforce per-slot acceptance and seal partial failures before the next dispatch.
- [x] Bind direct Go entry to the central fixed declaration; independently prove its grid/pairs/order.
- [x] Persist Docker ownership before work; bound and receipt parent cleanup of only the owned store.
- [x] Run deterministic functional/synthetic controls and one bounded disposable Docker forced stop; rebuild Go normal/race artifacts.
- [x] Record source/build/check receipts and protocol amendment; preserve sealed history and stop writes for independent review.

Both capture guards remain BLOCKED. No owner/comparator semantics, Rust, thresholds, numeric profile, pilot, benchmark, delegation, commit or PR changes are authorized.

See the [capture control amendment](references/response-owner-oneshot-readiness.md#capture-control-amendment-2026-10-27-pending-fresh-independent-review). Evidence is isolated in `/home/ashutosh/.cache/oneshot-capture-controls-20261027`; its bounded controls review is accepted. Capture remains blocked by the current P1/P2 findings and exhausted grants.

## Capability pilot authorization and pre-capture scope stop (2026-10-27)

- [x] Record parent acceptance of readinessreview03, Solclockaccountingreview02 and LunaP3precedencereview01, and exactly one 60-observation / at-most-eight-control / 180-second capability pilot authorization.
- [x] Inspect the actual runner before enabling guards. STOP before capture under the explicit scope condition: immediate disposition/identity/observer validation and forced-stop Docker cleanup confirmation need capture logic beyond the permitted guard/receipt amendment.
- [x] Preserve the frozen cache, executable sources and capture guards; run four existing focused declaration/authorization/budget methods. Zero pilot attempts, observations, workload controls, owner/synthetic children, builds or Docker calls. Close this authorization without an implied restart.
- [ ] Independent review of this scope-stop report before delivery acceptance or another decision; no delegation was authorized.

See the [new scope-stop report](/home/ashutosh/.cache/oneshot-capability-pilot-01-scope-stop-20261027/pilot-reference.md) and [current readiness amendment](references/response-owner-oneshot-readiness.md#capability-pilot-pre-capture-scope-stop-2026-10-27). Older pending checklists below retain their historical snapshot; parent review acceptance is recorded above. No performance gate or architecture decision follows.

## Final bounded reconciliation (2026-10-27)

- [x] Restrict comparator parity to generated Go Linux amd64 integer domain; disclose wider native usize wire behavior with executable controls.
- [x] Enforce concrete structured build commands/settings/config and sanitize/record child environments; retain corruption/inheritance regressions.
- [x] Rebuild combined Go normal/race/native/private-clock artifacts, verify identical checked binaries, run focused checks/readbacks and seal a fresh global receipt; stop checkout writes for review.
- [x] Scoped independent reviews accepted for the Go artifact/comparator, native clock semantics, standalone precedence, controls, and resource contract; pilot05 was separately authorized and is now spent. These acceptances do not complete the capability or clear the open P1/P2 findings.

See [final reconciliation](references/response-owner-oneshot-readiness.md#final-bounded-reconciliation-2026-10-27); prior snapshots remain historical.

## Readiness review01 reconciliation (2026-10-27)

- [x] Require declared save/enqueue plus independent durable item/keys/indexes/pending/accounting; preserve an intended omitted-write failure.
- [x] Bind bounded raw readback to child/selection/receipt and validate it in Python; retain prior command/authorization/sealing repairs.
- [x] Match i32/u32 wire ranges with boundary/rejection controls; keep loaded-v2 admission distinct.
- [x] Rebuild current artifacts, run focused normal/race/Python/Rust checks and reconcile historical/combined source provenance and counts in [readiness evidence](references/response-owner-oneshot-readiness.md).
- [x] Scoped independent semantics/fairness reviews accepted within their declared domains; the later pilot05 grant is spent. Both capture entries remain BLOCKED; memory INCONCLUSIVE.

## Bounded semantic correction (2026-10-27)

- [x] Preserve the uncommitted readiness prefix at base `e6c3f82c0146ed37ef626d739c75fc85057885b7`; no delegation/commit/PR/capture.
- [x] Bind native discovery, producer FIFO, Redis enqueue and conditional pointer clocks at their logical owner stages; retain the standalone explicit-clock API and original fixtures.
- [x] Consume the comparator's actual save accounting before cold connection; preserve genuine Go bootstrap errors and native NotSent.
- [x] Verify literal advancing-clock store effects, pending stage suppression, duplicates, consumer reads/latency, refused connection, existing controls and relevant replay; retain failures and hashes.
- [x] Scoped independent clock-semantics and standalone-precedence reviews accepted. Both future capture entries remain BLOCKED; no production parity claim.

See [bounded semantic correction](references/response-owner-clock-correction.md) for exact source scope, provenance, counts and historical acceptance qualification.

## One-shot readiness amendment (2026-10-27)

- [x] Preserve assigned T3 worktree/binding; verify clean/no unique commits and fast-forward to PR21 `e6c3f82c0146ed37ef626d739c75fc85057885b7`.
- [x] Read accepted native owner, actual Go primitives and prior measurement lessons; apply test authoring gate.
- [x] Build test-only reduced Go descriptor comparator and child-aware bounded capture support; no production/Rust/ABI/Lua/fixture changes.
- [x] Validate actual primitive/effect contracts, protocol, cleanup/failure artifacts and synthetic reconstruction; retain the observed wall-time mismatch as a readiness blocker.
- [x] Document every bypass/asymmetry and future exact 60-observation/at-most-eight-control declaration under 180 seconds.
- [x] Stop uncommitted for independent Luna + Sol fairness review. No delegation or capture authorized; actual capture requires blocker resolution and separate parent authorization.
- [x] Parent accepted the scoped comparator, Go artifact, and control reviews; capture remains blocked by unresolved capability/resource findings, not waived.

See [one-shot readiness protocol](references/response-owner-oneshot-readiness.md).
That historical prefix observed real Go wall-time normalization versus the
then-accepted single-clock native contract. The separately authorized clock and
P3 amendments later received scoped review acceptance; both future entries
still refuse capture because full capability and the open resource/lifecycle
findings remain unresolved.
This comparator is an experimental artifact, not a Go production latency baseline
or evidence of a native/language advantage. FFI_NO_GO/SCREEN_NO_GO retained;
Phase 1 partial, Phase 2 open, bridge unset. All activation/platform/license/
fencing/recovery gates remain open.

## Native SaveStep increment (2026-10-27)

- [x] Native ordinary StepRun preparation and SaveStep only; real authenticated Valkey and fresh Go readers.
- [x] Locally validate the synchronous Redis dependency, exact script fallback and bounded socket faults.
- [x] Record author evidence and prototype limits; final focused normal/race checks pass with no skips.
- [x] Parent acceptance after independent Luna-max and Sol interop reviews; neither found blockers.
- [x] Commit and open the accepted internal prototype as a stacked PR; discovery/enqueue remains the next layer.

See [native SaveStep](references/native-save.md) for scope, test gates and evidence. Broad Phase 1 remains partial, Phase 2 open, bridge unset.

## Native enqueue increment (2026-10-27)

- [x] Confirm clean T3 binding and fast-forward to accepted PR17 base.
- [x] Bounded native ordinary single-StepRun discovery enqueue and helper input cap repair.
- [x] Validate native sockets, unchanged fixtures and fresh Go-owner replay; leave uncommitted for independent Luna + Sol review.
- [x] Resume capacity-failed repair in place; validate four accepted native parity findings with additive real-owner cases.
- [x] Independent Sol review 03 and Luna review 02 found no blockers; parent accepted the bounded implementation. Sol passed 276 Go normal/race records plus five Rust tests with zero failures/skips and verified source/baseline hashes. Luna passed five Rust tests, fmt/clippy, and targeted Valkey cases in both queue modes; no replay/race rerun in that lane. Author's 926 Go pass records and four-target Rust compile checks are separate evidence.
- [x] Commit and deliver as [PR #18](https://github.com/ashutoshpw/inngest/pull/18), stacked on `t3/native-step-save-adapter` (PR #17); record its delivery metadata.

See [native enqueue](references/native-enqueue.md). Acceptance covers one ordinary single non-AI StepRun native SaveStep followed by native discovery enqueue and fresh Go readback/scanner replay. It does not establish production wiring, atomic save-plus-enqueue, exactly-once behavior, crash recovery, or active-lease recovery. Phase 1 partial, Phase 2 open, bridge unset; internal only, licensing unresolved.

## Bounded preparation memory increment (2026-10-27)

- [x] Verify clean assigned worktree/no unique commits; fast-forward to PR18 `e840eff6a`.
- [x] Inspect preparation/Go owners and test boundaries; retain validation and error order.
- [x] Bound retained compacted output before envelope allocation; preserve helper cap repair.
- [x] Author evidence: focused buffer, literal corpus, actual Go differential, save/enqueue integration, helper, and target builds.
- [x] Luna review 01 found no correctness blockers; parent accepted the buffer fix.
- [x] Commit, push, and open the documentation-reconciled fix as [PR #19](https://github.com/ashutoshpw/inngest/pull/19), stacked on PR18.

See [bounded preparation memory](references/bounded-preparation-memory.md). Data
compaction retains at most 4,194,295 bytes and prepared output at most 4,194,304
bytes; these are per-buffer limits. Full response validation and AI-marker
scanning continue past the output cap; AI/ineligibility rejection still precedes
output-size rejection, which still precedes state-size rejection. Input/parser/
whole-process memory remains unbounded. PR18 still has separate
preparation/save and enqueue adapters orchestrated by Go tests; it is not a
complete native response owner. Existing migration gates remain open.

## Test-only Go StepRun input binding (2026-10-27)

- [x] Verify clean assigned worktree/no unique commits; fast-forward to PR19 `4a870dd35`.
- [x] Audit actual response/RunContext sources and retained owner witnesses.
- [x] Add private prospective source binding and actual SaveStep observations; reuse existing literals.
- [x] Finish focused normal/race checks and record source/log hashes.
- [x] Repair review01 source-preference coverage and narrow snapshot claims; retain 139 baseline checks and four controls, add four distinct-response cases and four controls.
- [x] Independent Luna review02 found no blockers; parent accepted the narrowed test-only contract. Author and independent 144-record normal/race runs are reported separately; Luna inspected eight existing mutation logs without rerunning them.
- [x] Commit and push the accepted tests/evidence; open [PR #20](https://github.com/ashutoshpw/inngest/pull/20) against PR19's `t3/native-prepare-memory-bound` branch using the required Description, Release note, and Migration note sections. Link PR19 and PR20 to the delivery thread.

This is prospective test evidence only; it does not claim a complete Go owner,
production selector, native adapter, full source coverage or ownership transfer.
At PR20 delivery, native follow-on required a parent architecture decision: prove pre-effect
eligibility excludes relevant lifecycle/capacity/tracing effects, or preserve them
through a single ordered Go handoff. The constructed test-only choice is now authorized below; production ownership remains unselected.
See [input binding](references/step-run-input-binding.md). Phase 1 partial,
Phase 2 open, bridge unset; PR18 remains separate native adapters, FFI NO_GO remains.

## Constructed native response owner (parent amendment, 2026-10-27)

- [x] Preserve assigned binding; verify clean/no unique work and fast-forward to PR20 `70e6536f206b2dc9253a09e6d1ffb37bd8d035eb`.
- [x] Inspect source owners, skills, and accepted preparation/save/enqueue/input evidence.
- [x] Implement one test-only native invocation owning preparation, accounting, accepted save/pending, conditional discovery/enqueue and disposition.
- [x] Establish constructed exclusive domain from actual setup before strict native projection; retain all exclusions and SCREEN_NO_GO.
- [x] Validate real-store faults, explicit native retries and fresh ordinary Go replay; preserve twelve baseline files.
- [x] Record exact validation/hash evidence in [native response owner](references/native-response-owner.md), including author and independent review scope.
- [x] Independent Sol interoperability review 01 and Luna semantics review 01 found no blockers; parent accepted the bounded constructed-domain owner.
- [x] Commit and deliver the implementation/evidence as [PR #21](https://github.com/ashutoshpw/inngest/pull/21), stacked on PR20; link it to the delivery and parent threads.

Reduced-domain ownership and fresh-Go interop only. No production eligibility,
observer equivalence, full handler parity, traffic, transport selection, benchmark,
SDK change, queue consumer, fencing or activation. Go metrics, noop-span and clock
operations are excluded. Metadata proposals/SavePending/reset are unnecessary in
the admitted single StepRun/loaded-v2/no-force-plan domain and remain unimplemented.

## Delivery

Use dedicated worktrees and small stacked PRs. Verify and record the first layer's base before implementation; each later layer targets its preceding branch. Follow the repository's conventional-title and PR-text rules in [validation and rollout](references/validation-rollout.md).

## Work packages

- [ ] **1. Freeze baseline — partial.**
  - [x] SDK request wire and replay-shaped input corpus implemented and independently Luna-reviewed with no blockers; malformed-case and reset-overhead-label findings are fixed. See the [corpus README](../../../../pkg/execution/driver/testdata/sdk-request-v1/README.md).
  - [x] Opcode-plan and generator-parser fixtures accepted after independent Sol-high review found no blockers; three P2 findings were fixed, with Go validation and mutation evidence in the [opcode baseline](references/opcode-plan-baseline.md).
  - [x] CEL/SQL portable fixtures and owner-boundary tests accepted as stack layer 2 after independent Sol-high review; the benchmark path was renamed to `evaluator_program_construction_cached_parse`, its source comment explains lifted-literal parser-cache reuse, and ten samples were rerun. See the [CEL/SQL compatibility baseline](references/cel-sql-compatibility.md).
  - [x] Queue-item JSON and representative initial run-state fixtures accepted by the parent as partial stack layer 3 after independent Sol-high review; the sole P2 finding on loaded `PriorityFactor` and `ParallelCoalesceKey` is fixed and revalidated. The recovery witness is a separate accepted layer 4 and is not part of layer 3. See the [queue/state compatibility baseline](references/queue-state-compatibility.md).
  - [x] SDK response -> checkpoint -> fresh-runtime replay witness accepted after independent Luna-max review found no blockers; real HTTP, surviving stores, worker retry, and both key-queue modes are covered. This replaces drained owners, not an OS process or active lease. See the [recovery baseline](references/recovery-baseline.md) and [fixture README](../../../../tests/execution/executor/testdata/sdk-response-replay-v1/README.md).
  - [x] Bounded pure opcode bridge entry gate satisfied: 27 cases/all 17 tags, literal original indices and payload identity; focused/race checks and independent Luna review accepted with no blockers. See the [bridge contract](references/opcode-bridge-contract.md) and [bounded Go service evidence](references/go-service-baseline.md).
  - [x] Accept the bounded staged-batch Go owner corpus and bounded component-cost evidence. Literal output/recipe/barrier/version/clock witnesses and forty Go-only component samples passed focused/race checks; independent Luna round two found no blockers after the sibling-concurrency P2 repair. Keep the cost STOP: full staged projection/validation/materialization costs and absolute sleep clocks remain unmeasured. A future Rust-owned continuation with Go effect ports needs a separately implemented and reviewed Go response-owner extraction; this layer does not implement or approve that extraction. See the [staged batch reference](references/staged-batch-reference.md).
  - [x] Establish the bounded Go-only native-step persistence oracle; Sol's queue review and Luna's state review were independently accepted with no blockers. Focused normal/race selections pass with 173 executor and 111 Redis-state cases and no failures or skips, including actual Valkey coverage across both queue modes, layouts, prefixes, state faults, queue indexes, and tombstone expiry. PreparedStepRun bytes are literal-pinned. The assertion limits and future-adapter requirement are recorded in the [native-step Go baseline](references/native-step-go-baseline.md). Fresh Go runtime replay is now accepted; preliminary native dependency metadata is recorded separately, while actual runtime qualification and license reconciliation remain open. Broad Phase 1 remains partial, Phase 2 open, and the bridge unset.
  - [x] Extend the existing Go recovery owner with terminal native-step replay across normal discovery, discovery failure before commit, and committed ACK suppression. New and unchanged recovery selections pass separately in both modes, normal/race; independent Luna review found no blockers and the parent accepted the baseline. See [native-step replay](references/native-step-replay.md).
  - [ ] Complete the Go reference baseline: SDK-response replay, CEL/SQL server semantics and broader operator coverage, broader queue/state persistence and recovery, broader lifecycle/fault coverage, and initial Go performance baselines; confirm SDK/platform policy with owners. CEL/SQL and queue/state have partial Go fixtures. Rust parity and Go/Rust/bridge comparisons belong to later phases. See the [CEL/SQL compatibility baseline](references/cel-sql-compatibility.md).
- [ ] **2. Bridge spike — implementation and amd64 evidence accepted; Phase 2 remains open; bridge choice unset.** The isolated, build-tagged Rust/process/FFI prototype and correctness/build evidence are recorded in the [opcode bridge spike implementation](references/opcode-bridge-spike.md). Independent Sol review accepted the measurement evidence: all 21 commands, 288 latency rows across three seeds, 180 serial benchmark repetitions, source/raw hashes, and calculations match with no blockers. The uncalibrated proposed thresholds still show substantial decoded/process overhead and were not relaxed. The report also records why the median-based full-response FFI result is not a robust selection signal. Native arm64 execution and the final GNU Cgo link/runtime check remain open; broad Phase 1 is partial, Phase 2 is not complete, and no bridge is selected. See the [human results report](references/opcode-bridge-spike-results.md) and [structured results](references/opcode-bridge-spike-results.json).
- [x] **2a. Direct-buffer FFI — safety, fairness, and representative amd64 screening accepted; stop boundary expansion.** Independent Luna safety and evidence reviews found no blockers; rust-direct-ffi-screening-review-01 verified the 54 paired ratios, 18 summaries, 63 cached artifact hashes, source/binary manifests, and serial summaries. Sol accepted the unique-job-index fairness fix and ran the representative screen: all 15 screening and seven setup/build commands passed, with 72 latency rows and 60 serial repetitions. Only 2/6 direct cells pass the unchanged, uncalibrated criteria. The follow-on fixed response-owner boundary screen is independently reviewed and parent-accepted as SCREEN_NO_GO; stop current FFI-boundary expansion pending a separate architecture assessment. Phase 1 stays partial, Phase 2 open, bridge unset, and production wiring absent; this result does not establish broad Rust infeasibility. See the [implementation](references/opcode-bridge-direct-ffi.md), [screening evidence](references/opcode-bridge-direct-ffi-screening.md), and [response-owner screen result](references/response-owner-screen-results-2026-10-27.md).
- [ ] **3. First Rust slice — isolated model and bounded real-effects integration accepted; production activation remains gated.** The test-only Rust driver exercises real Go effect ports for the eligible whole-batch continuation. Future production candidate: None/Step/StepRun/StepError/StepFailed/StepPlanned only; any excluded or invalid descriptor keeps the whole batch in Go. Parent acceptance covers only the isolated model and test-gated effects integration; it does not activate Rust, select a bridge, establish performance, or complete the broader migration phases.
  - [x] Implement and validate the fixed 18-row rejection-screen controls, declaration, package deadline, and independent screen decision path. The controls were accepted in review 03; see the [screen protocol](references/response-owner-screen.md).
  - [x] Repair and validate capture-evidence STOP handling (cooperative owned snapshot, streamed/checked child evidence, strict reducer admission, and cleanup reserve within 900s); included in accepted controls review 03. See [STOP repair](references/response-owner-screen.md#accepted-capture-evidence-blocker-repair-2026-10-27).
  - [x] Parent accepted Luna round 3 with no blockers and authorized exactly one fixed screen. It completed 18 rows/9,594 validated slots, including 9,000 steady slots, one 14-contract functional preflight, six one-request port preflights, and 25 COMPLETE child receipts in 90.037s, with no retries, timeouts, or retained inputs. Independent Luna result review 01 was accepted by the parent with no blockers; it recomputed raw evidence and confirmed byte-identical derivation. Every workload fails p95, closed-loop, and owner-service gates in every seed; memory remains INCONCLUSIVE. See the [screen results](references/response-owner-screen-results-2026-10-27.md).
  - [ ] Next architecture assessment pending. Stop current FFI-boundary expansion; no additional capture, profile, optimization, or process implementation is authorized by this result. Phase 1 remains partial, Phase 2 open, bridge choice unset, and production activation absent. Deferred process comparison, native arm64, and activation gates remain open.
  - [x] Implement the matched response-owner measurement harness and retain its tiny-smoke/normal/race evidence. Parent accepted the matched-port, timing-boundary, raw-reproduction, bounded-input-retention, and service-rate reviews; Sol review 03 found no service-guard blockers, and the parent inspected all six RSS-boundary guards plus the strict-enclosure edge fix. Harness acceptance is limited to measurement tooling and evidence; it establishes no performance result, transport choice, or activation. See [measurement protocol](references/response-owner-measurement.md).
  - [x] Repair accepted P2 measurement input retention with one independent input per active slot, separate backend precreation, explicit expanded throughput scope, raw ownership accounting and fail-closed legacy memory eligibility; focused normal/race, negative controls and tiny smoke validated. No improvement claim.
  - [x] Repair accepted P2 throughput scope: retain closed-loop >=0.90 and add separate owner-service-rate >=0.90 guard with unchanged per-cell median/2-of-3 rule; reproduce the false PASS, reject invalid resource durations and rederive existing smoke to a new provenance-backed schema-3 result. No capture/binary change and no improvement claim.
  - [x] Repair accepted Luna P2 boundary eligibility in reducer/tests/docs: require all six captured lifecycle snapshots with usable RSS and valid times; retain dense sampling, bounded retention and schema-3 service-rate guard. Correct full-evidence regression fixtures and rederive existing smoke exclusively. See [cumulative repair evidence](references/response-owner-measurement.md#p2-boundary-snapshot-repair-2026-10-27).
  - [x] Parent accepted the cumulative response-owner measurement harness after the final Sol throughput/raw-derivation and Luna timing/retention/strict-boundary reviews, authorizing one declared campaign attempt on 2026-10-27.
  - [x] Parent accepted the independent Sol stop audit: the first native/controlled/run64k/C1 campaign row expired at 110s after 3,419/5,033 completions; sustained progress was observed, but no request timeout or native hang was established. Preserve the original `stuck`/`timeout=true` fields and all 118 original-bundle hashes. The campaign remains incomplete, with no performance verdict. See [stopped campaign evidence](references/response-owner-measurement-campaign-2026-10-27.md).
  - [x] Implement the bounded capture-budget/diagnostics amendment, validate its recorded focused normal/race/control checks, and execute exactly one seed-26011 native/controlled/run64k/C1 diagnostic. Under 210/240/260/270s watchdog/context/test/external budgets and the unchanged 20s request deadline, all 5,033 slots validated and closed at 156.080s, with zero retained inputs. The 152,453 periodic samples have a 17.352ms maximum gap, so RSS is INCONCLUSIVE under the unchanged 5ms limit; whole-host RSS is not precise Go-versus-Rust attribution. Luna's final control/source/evidence review found no blockers and the parent accepted this diagnostic evidence only. The one-row authorization is exhausted; no comparative gates were derived. The next measurement decision is pending separate authorization. See [diagnostic amendment](references/response-owner-diagnostic-2026-10-27.md).
  - [x] Implement and validate real Go effects plus a test-gated native continuation driver; delivered in [PR #12](https://github.com/ashutoshpw/inngest/pull/12), targeting `t3/response-continuation-model` (PR #11). See [real effects integration](references/response-real-effects.md). The same 40 literals and lifetime/poison witnesses pass. Sol's earlier independent review passed six actual Valkey storage/commit-error cases in both key-queue modes and persisted replay in both modes; these were not rerun after the final driver-only close/admission repair. A broad attempt stopped on Docker permission before Valkey; the explicit non-Valkey selection passed 20 normal/race repetitions. No production activation or bridge selection.
  - [x] Luna's initial native review and Sol's initial Go review identified the async callback snapshot and close/dispatch admission findings. After both repairs, Sol's fresh Go reviews found no remaining blockers and the parent accepted the bounded integration. See the exact test evidence and limits in [real effects integration](references/response-real-effects.md).
  - [x] Implement and locally validate the isolated six-tag response-continuation model and independent real-Go/shared literal contract; see the [model reference](references/response-continuation.md).
  - [x] Independent combined Luna-max round-two review found no blockers and the parent accepted the isolated model plus 40-literal shared contract. The later test-gated real-effects integration is accepted separately; production host integration and activation remain gated. Phase 1 remains partial and Phase 2 open.
  - [x] First bounded Go response-owner extraction accepted after Sol effects review and independent Luna-max staging round two; delivered in [PR #10](https://github.com/ashutoshpw/inngest/pull/10), stacked on PR #9. The real response entry uses a private runner for staging/group joins and accepted-save continuations, concrete Go effects, and checkpoint reuse. Baseline/current/race and extraction integration witnesses are recorded in the [response-owner extraction](references/response-owner-extraction.md). This does not activate or select Rust; retryable/planned sequencing, finer queue ports, host lifetime and recovery remain open.
- [ ] **4. CEL/SQL and core parity.** Have Rust consume the CEL/SQL fixtures and compare evaluation plus SQLite/Postgres filter behavior against the Go baseline; then migrate the remaining trigger, schedule, and run/step decisions.
- [ ] **5. Persistence and recovery.** Port queue/state algorithms only after persisted formats, atomicity, ownership, and recovery pass parity; Go remains the sole Goose migration owner.
- [ ] **6. Fenced rollout.** Expand shadow and production ownership by run/partition only after parity and recovery gates pass.
- [ ] **7. Retire Go core.** Retire Go algorithms after parity, deferred-platform validation, release, recovery, and rollback gates pass; retain a validated Go rollback artifact.

## Acceptance criteria

- [ ] Full wire/replay, CEL/SQL, queue/state, fault, recovery, and lifecycle corpus has no unexplained differences; effect ordering and idempotency behavior match.
- [ ] Before rollout, matched Go/Rust/bridge performance is measured and gates calibrated; no migration performance threshold is inferred from the current Go microbenchmarks.
- [ ] Supported-target builds, compatible persisted state, single ownership, staged rollout, and rollback are validated before Go retirement.

## References

- [Consolidated pilot05 audit](references/response-owner-capability-pilot-05-audit.md) — independent evidence acceptance, incomplete coverage, open P1/P2 findings, and current gates.

- [Capability pilot03](references/response-owner-capability-pilot-03.md) — one exhausted attempt, resource failure/partial counts, cleanup, four-role provenance and immutable source lifecycle.

- [Authorization state repair](references/response-owner-authorization-state.md) — historical gate inventory and complete future activation protocol, accepted by parent before pilot03.

- [One-shot readiness and review01 reconciliation](references/response-owner-oneshot-readiness.md) — durable-evidence/range repairs, final checks/counts and combined provenance; capture blocked.

- [Bounded clock/accounting correction](references/response-owner-clock-correction.md) — separately authorized owner-stage semantics and retained evidence; capture blocked.

- [Native response owner](references/native-response-owner.md) — parent-amended test-only exclusive domain, one-invocation effects, real-store/replay proof, accepted validation, delivery and remaining gates.

- [StepRun input binding](references/step-run-input-binding.md) — prospective Go-only source facts, actual effect observations, authoring gate, checks and limits.

- [Native enqueue](references/native-enqueue.md) — bounded discovery implementation, provenance, validation and open gates.

- [Native SaveStep](references/native-save.md) — internal prototype scope, parity, runtime faults, commands, review handoff and open gates.
- [Native dependency inventory](references/native-save-dependencies.json) — exact additive lock entries and upstream license/MSRV metadata; no distribution grant.

- [Fixed response-owner screen](references/response-owner-screen.md) — exact future scope, counts, budgets, decisions, stop rules and implementation review.
- [Fixed screen results](references/response-owner-screen-results-2026-10-27.md) — completed one-screen NO_GO, every seed ratio, diagnostics, memory eligibility, hashes and independent-review STOP; links structured summary and CSVs.
- Fixed-screen data: [JSON summary](references/response-owner-screen-results-2026-10-27.json), [seed ratios](references/response-owner-screen-results-2026-10-27-pairs.csv), [row diagnostics](references/response-owner-screen-results-2026-10-27-rows.csv), [RSS boundaries](references/response-owner-screen-results-2026-10-27-rss-boundaries.csv) — exact completed capture data and provenance.

- [Bounded response-owner diagnostic](references/response-owner-diagnostic-2026-10-27.md) — accepted budget amendment, one-row capture, checks, provenance and limits.

- [Stopped response-owner campaign](references/response-owner-measurement-campaign-2026-10-27.md) — incomplete first-row STOP, preserved raw classification and no performance verdict.

- [Matched response-owner measurement](references/response-owner-measurement.md) — accepted boundaries, profiles, capability, smoke evidence and remaining fairness gates.

- [Real effects integration](references/response-real-effects.md) — bounded scope, ABI/ownership, authoring gate, validation and open host gates.

- [Response-continuation model](references/response-continuation.md) — isolated ownership/API, shared fixtures, checks, limits and host integration gates.

- [Response-owner extraction](references/response-owner-extraction.md) — bounded Go staging/join and save-continuation work, invariants, verification, and remaining Rust integration gates.
- [Direct FFI representative screening](references/opcode-bridge-direct-ffi-screening.md) — accepted independent safety, fairness, and Luna evidence reviews; 2/6 direct cells pass and full-matrix expansion stops.
- [Direct FFI structured screening](references/opcode-bridge-direct-ffi-screening.json) — exact command/status/hash and resource audit, all 54 paired comparisons, 18 summaries, and 60 serial repetitions.
- [Direct FFI paired runs](references/opcode-bridge-direct-ffi-screening-paired.csv) and [serial repetitions](references/opcode-bridge-direct-ffi-screening-benchmarks.csv) — all 54 ratios and 60 benchmark repetitions.

- [Direct-buffer FFI experiment](references/opcode-bridge-direct-ffi.md) — ABI safety reasoning, test gate, correctness and smoke checks, retained limits, and review handoff.
- [Staged batch Go reference](references/staged-batch-reference.md) — accepted bounded owner corpus, sibling-concurrency mutation evidence, component-cost STOP, and separate extraction gaps.
- [Staged component samples](references/staged-batch-cost-benchmarks.csv) and [raw benchmark output](references/staged-batch-cost-benchmarks.txt) — forty Go-only component samples and original byte counts; full projection, validation, and materialization costs remain unmeasured.

- [Runtime map and proposed boundary](references/runtime-map.md) — ownership, bridge scope, and decision boundary.
- [Behavior and persistence contracts](references/behavior-contracts.md) — observable replay, CEL, lifecycle, queue, and storage behavior.
- [SDK and deployment matrix](references/sdk-deployment-matrix.md) — configured CI/build coverage, first Linux bridge targets, and deferred gates.
- [Opcode-plan baseline](references/opcode-plan-baseline.md) — fixture scope, review status, test evidence, mutations, and benchmarks.
- [Bounded opcode bridge contract](references/opcode-bridge-contract.md) — sequencing amendment, index representation, audit rationale, validation, and accepted bounded entry gate.
- [Opcode bridge spike implementation](references/opcode-bridge-spike.md) — isolated Rust kernel, process/FFI transports, correctness and packaging evidence, limits, and remaining gates.
- [Opcode bridge spike measurement campaign](references/opcode-bridge-spike-measurements.md) — accepted harness review, campaign scope, schema, environment, and remaining gates.
- [Opcode bridge spike measurement results](references/opcode-bridge-spike-results.md) — per-route threshold outcomes, serial benchmarks, ordered startup observations, resource caveats, and exact command statuses.
- [Opcode bridge spike structured results](references/opcode-bridge-spike-results.json) — all per-run latency/resource rows, paired ratios, benchmark repetitions, command details, and raw artifact hashes.
- [Go service baseline](references/go-service-baseline.md) — accepted bounded DB/Valkey/lifecycle evidence at the PR #4 snapshot, commands, skips, and limits.
- [CEL/SQL compatibility baseline](references/cel-sql-compatibility.md) — portable CEL evaluation and SQLite/Postgres filter fixtures, owner-boundary tests, and Go benchmark observations.
- [Queue/state compatibility baseline](references/queue-state-compatibility.md) — persisted queue-item and representative initial run-state values, owner-boundary tests, and Go microbenchmarks.
- [Recovery baseline](references/recovery-baseline.md) — accepted surviving-store replay witness, validation, and limits.
- [Phase 1 baseline discovery](references/baseline-discovery.md) — source-grounded coverage and remaining baseline work.
- [Validation and rollout](references/validation-rollout.md) — stack order, parity commands, rollout, release, and rollback gates.

## STOP admission repair (2026-10-27, current)

- [x] Reproduce the accepted setup/pre-first admission race with bounded test-only barriers.
- [x] Linearize request admission and owned failure snapshots across setup and all phases.
- [x] Verify real Python process-group STOP through the actual Go parent and tiny row; normal/race focused Go and Python controls.
- [x] Record exact evidence and limits; leave uncommitted for fresh independent review.

- [Native-step Go baseline](references/native-step-go-baseline.md) — accepted amendment, test-authoring gate, bounded persistence evidence and remaining gates.

- [Native-step replay](references/native-step-replay.md) — parent-accepted fresh Go owner replay, three scenario evidence, eight passing validation commands, preliminary Redis client qualification, and remaining gates.

## Resource contract amendment (2026-10-27)

- [x] Bind owner accounting to supervisor/runner/spawn PID + start ticks; unknown discovery fails closed.
- [x] Make only verified separate-store FD enumeration diagnostic; require authenticated INFO and all other store fields.
- [x] Run one deterministic no-process fixture package within 30 seconds; preserve 1,112 history files and all pilot04 files.
- [x] Seal source delta and evidence; parent later accepted its bounded resource-contract review. Remaining P1/P2 lifecycle findings stay open.

Reference: [Resource contract amendment](references/response-owner-resource-contract.md).

## Resource review P2 repair (2026-10-27)

- [x] Inspect resource protocol and collector/Go test ownership; preserve entry source and historical hashes.
- [x] Bind spawn receipts to the current grant, runner generation and declared active invocation; revalidate cached identities.
- [x] Carry strict capacity failures with partial resource batches; isolate null authorization fixture from current spent registry.
- [x] Run bounded no-process fixtures, focused Go normal/race and compile; seal fresh source/build evidence.
- [x] Parent accepted the bounded resource-contract review; no new authorization, capture or performance claim follows.

Reference: [Resource contract amendment](references/response-owner-resource-contract.md).
