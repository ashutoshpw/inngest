# Collector review closure (2026-10-27)

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

Parent explicitly accepts the FD repair and **resourcebatchreview02 P2 closure**.
The collector source, tests and sealed repair caches are unchanged by this
acceptance. Pilot03 remains spent with its original exit mechanism unproven;
its historical 699.441543/699.460719 ms gaps keep memory INCONCLUSIVE.
The separately authorized [pilot04 attempt](response-owner-capability-pilot-04.md)
is now exhausted after live Valkey FD EACCES, five validated observations and
one terminated row. No general FD-denial exemption or algorithm/privilege/policy
change was made. Independent review of pilot04 results remains pending.
All repair-scope/pending-review prose below is preserved historical context.

# Collector-only repair amendment (2026-10-27)

## Bounded P2 evidence repair — closure pending independent review

2026-10-27, implemented and uncommitted in `t3-7411bca6` /
`t3/readiness-comparator-harness`. Prior Luna review accepted the FD classifier
and synthetic real-exit/live-denial controls. This amendment repairs only the
remaining aggregation/traversal evidence P2; it does not claim review acceptance.
The original repair record below is historical, including its single already-run
synthetic package. **No additional real-child package was run.**

`resources()` now converts task enumeration and per-thread children-read
`OSError` into `ProcSampleError`, retaining parent, runner, all earlier children,
the current sampled process and a separate unavailable traversal record. That
record carries the sampled PID/start-tick/credential identity, role, failed field,
attempted path, full OS filename when supplied, error type/text/errno and start,
end and wall-clock timestamps. It adds no memory value. Task iterator failures
are caught during iteration as well as creation. ENOENT previously disappeared
through `FileNotFoundError` catches; it now fail-stops with truthful unavailable
coverage. A process or task disappearing alone cannot prove complete descendant
coverage. It is not reclassified as normal exit or zero memory, even if earlier
process sampling observed exit. The independently verified same-identity FD exit
exception and all PID start-tick defenses remain unchanged. Proc field errors
also retain the full OS filename (`str(OSError)` plus `filename`), since repr can
omit it. Status-file denial remains fatal even when stat confirms exit.

The old monitor-only fixture stubbed `resources()` and did not prove aggregation.
It is replaced by a controlled proc tree and provider operations: parent 100,
runner 101, prior child 202, later child 303. The real collector, aggregator,
monitor and `capture_bundle` finalizer run with synthetic provenance/process/store
operations inside a protected mock context. No public guard changes or test-only
production API were added. Seven cases cover later-child FD EACCES plus task and
children EACCES/EIO/ENOENT. They assert ordered collected samples, a separate
failed traversal record where applicable, field identity/error/timestamps,
monitor flushing, the FINAL STOPPED capture manifest's `failed_resource_samples`,
and every sealed artifact hash. A direct resources fixture also proves the
monitor's remembered PID start ticks are honored, keeping earlier records and
clearing exposed metrics on mismatch. A direct status-OSError fixture proves
fatal behavior despite established exit and full filename retention.

Test authoring rationale: these protect evidence retention, fatal-error and
identity contracts at the owning boundary. Existing proc-record cases alone and
the old stubbed monitor case could not detect lost aggregation or traversal
exceptions. Controlled filesystem operations supply raw reads/errors, rather
than fabricated collector receipts. Regression execution on the pre-P2 source
produced six intended traversal failures (ordinary exceptions or silently
ignored ENOENT) and two missing-filename errors; that log is retained. After the
repair, six collector methods pass, including seven finalization subcases. Five
state-applicable existing methods also pass: declaration, package reserves, raw
reconstruction/corruption, fixed resource estimator and build-contract corruption.
Known null-registry tests were not rerun or changed: the registry is spent, as
recorded in the original repair evidence. Direct public `capture` and
`capture_bundle` checks both reject it with `capture authorization exhausted`.
Python compilation and whitespace checks pass. No Go/Rust build/test, benchmark,
owner, store, actual capture, grant, privilege change, delegation, commit or PR.

New evidence: `/home/ashutosh/.cache/oneshot-collector-p2-20261027/`.
`baseline-regressions.log` records the pre-fix failures; `fixtures-retained-final.log`
and `focused-python.log` record actual runs. `synthetic-final-artifacts/` retains
all seven synthetic final manifests/resource streams and their sealed artifacts;
these are fixture executions, not captured owner measurements. `entry.json`,
source patches, `verification.json`, `source-delta.json` and `evidence-sha256.json`
separate this source delta from historical build provenance. The original 982
historical files (including all 30 pilot03 capture files) and all 28 files in the
latest prior repair cache are verified unchanged. These counts refer to existing
sealed evidence; no historical file/receipt is refreshed. The accepted FD
classifier and capture finalizer are inspected unchanged, not newly qualified by
real execution. Manifest sealing needed no change for this path. Existing
best-effort limits remain: full media, interrupted writes, SIGKILL or power loss
do not guarantee a successfully written/sealed artifact.

Pilot03 remains spent; its original exit mechanism is unproven. Memory remains
**INCONCLUSIVE** with historical maximum parent/runner gaps
699.441543/699.460719 ms. This P2 evidence repair confers no capture authorization
or performance conclusion. Closure remains pending parent-owned independent
review; writes stop after the new evidence seal.

## Original repair record (historical)

Implemented, uncommitted; **pending parent-owned independent review**. Scope is
Python collection, fixture tests and one <=30-second synthetic package only in
`t3-7411bca6` / `t3/readiness-comparator-harness`. No owners, Valkey, capture,
pilot, benchmark, grant, capability/policy change, root, delegation, commit or PR.
The pilot03 source registry and permanent claim/runner markers remain unchanged
and spent. Historical source/build receipts describe their original bytes;
this repair does not refresh any Go/native build receipt or authorize execution.

## Evidence and classification

Read the one-shot protocol/readiness reference, pilot03 report and raw STOPPED
manifest/resource/coverage/final-audit evidence. Pilot03 remains exactly one
attempt, five validated observations and one cancelled row. Its original failure
is `PermissionError: [Errno 13] Permission denied: '/proc/1327265/fd'`.
Its 35 retained child samples have the same UID 1002 / GID 999 as that capture's
parent/runner. The failed sample discarded stat/status/FD ownership, so exit-time
mm teardown is plausible, **not proven**. The synthetic result below demonstrates
a possible exit mechanism; it does not diagnose or reclassify pilot03.

The original stream still has maximum parent/runner sampling gaps of
699.441543/699.460719 ms. Memory remains **INCONCLUSIVE**. No scheduling or
performance overhaul, resource-estimator change, density waiver, fabricated
memory zero, owner/store experiment or performance verdict accompanies this repair.

## Collector semantics

`proc_record` retains raw stat/status, start ticks, credential identity, RSS/HWM
and thread observations before FD enumeration, plus independently timestamped
field availability/error/errno and FD-directory ownership when permitted. A
second stat read checks start ticks and state. EACCES/ENOENT/ESRCH on FD reads
can complete only with independently observed exited state (Z/X/x) and matching
identity; missing memory remains missing. Live denial, other read errors,
unverified disappearance, missing live memory and identity changes fail-stop.
Raw partial fields survive on `ProcSampleError`; identity mismatch clears exposed
metrics while preserving raw field evidence, so another process is never charged
to the expected identity. The monitor retains previously observed PID start ticks
and flushes the whole partial resource batch before raising. Command and capture
failure receipts retain the failed sample, including failures before streaming.
The runner's terminal record uses actual Popen poll/wait lifecycle after reaping;
it does not reread a possibly reused PID or synthesize RSS. The existing process
group survivor check remains. Existing estimator and scheduling/density constants
are unchanged; no broad permission-error exemption remains, including for the
separate store role.

## Authorized single synthetic package

Command (run once):

```sh
python3 scripts/response-owner-collector-controls.py /home/ashutosh/.cache/oneshot-collector-repair-20261027/synthetic-once
```

One launcher PID 1403095, three sequential children, no retries/replacements.
UID/GID **1002/1002**, ordinary same-UID permissions; this runner's GID differs
from the historical capture's Docker-capable GID 999. No privilege escalation.
The only dumpability change is PR_SET_DUMPABLE=0 by the third child on itself.
Total internally measured package interval, including import/setup, sampling,
cleanup and per-control seals: **0.133248984 seconds**, below 30 seconds.
Final receipt writing follows the emitted end timestamp (the tool returned in
0.2 seconds); this is not claimed as a separately instrumented final-fsync time.
A 25-second work deadline and 29-second alarm bound the package, with bounded
acknowledgements, log lines, child lifetimes and cleanup. A failed control is
retained and stops further dispatch.

- Live readable, PID 1403096: PASS, 0.036361202s; RSS 14996 KiB, three FDs.
- Coordinated normal exit held unreaped, PID 1403104: PASS, 0.032662044s.
  WNOWAIT independently records exit 0; start ticks match before/after, stat state
  Z, FD directory UID/GID 0/0 and actual FD EACCES (13). RSS/HWM are null, not zero.
- Live nondumpable denial, PID 1403105: PASS, 0.037705693s; acknowledged dumpable=0,
  same start ticks/state S, actual FD EACCES (13), FD ownership 0/0 and retained
  RSS/HWM 14912 KiB. The collector raises the expected fail-stop. It is not treated
  as exit or a valid complete memory observation.

All children exit 0, are reaped and confirmed absent. All five raw samples,
including the expected failed live-denial sample, stdout, argv, timings,
ownership/errors and per-control receipts are retained. Zero owner launches,
zero stores/store operations, zero capture attempts. No synthetic replacement
was used to obtain exit EACCES; ENOENT/readable outcomes would have been recorded
as observed. Deterministic fixtures separately cover those paths.

## Validation and evidence boundaries

Evidence directory: `/home/ashutosh/.cache/oneshot-collector-repair-20261027/`.
`entry.json` retains pre-repair source/history hashes and `collector-before.py`
the original collector. `source-delta.json`, `verification.json` and
`evidence-sha256.json` record current hashes, immutable-history verification and
new receipts; these are **source deltas, not fresh all-build provenance**.

The test authoring gate protects independently observable retention, fail-stop,
identity and sealing contracts at the proc collector boundary. Existing estimator
fixtures do not exercise proc permission/lifecycle races. Filesystem fixtures
supply kernel reads/errors, not preconstructed successful collector receipts;
there is no added test-only production API. The optional expected-start-tick
argument is used by the real monitor.

Four focused collector fixture methods pass: partial retention for live/ambiguous
EACCES/ENOENT/ESRCH; same-identity exit with absent memory, including readable FDs;
PID reuse/disappearance/previous-identity mismatch; and flushed failed samples
before monitor fail-stop. The partial-retention regression fails on the original
collector at FD EACCES and passes after repair. Five applicable existing Python
methods pass (declaration, reserves, raw reconstruction, estimator and build
contract corruption); compilation and whitespace checks pass. No Go/native tests
or builds run.

All development failures remain in the evidence directory. An initial baseline
adapter failed to redirect the original module's proc path; its corrected run
reproduces the raw FD PermissionError. An initial eight-method existing-test
selection mistakenly included three null-authorization methods: six assertions
fail because the source registry contains the spent pilot03 grant. The exact six
assertions also fail with the original collector and unchanged registry;
`authorization-baseline.log` retains that proof. No registry/test expectations
were changed to hide these failures. Final applicable selection passes five
methods. The existing unrelated plan-status issue is untouched.

Independent review and any future measurement decision remain with the parent.
No capture capability or memory acceptance follows. Writes stop after final
source/evidence sealing; all older report prose and captures remain historical.
