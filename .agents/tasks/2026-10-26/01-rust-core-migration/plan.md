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
Native follow-on requires a parent architecture decision: prove pre-effect
eligibility excludes relevant lifecycle/capacity/tracing effects, or preserve them
through a single ordered Go handoff. Neither is selected or implemented here.
See [input binding](references/step-run-input-binding.md). Phase 1 partial,
Phase 2 open, bridge unset; PR18 remains separate native adapters, FFI NO_GO remains.

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
