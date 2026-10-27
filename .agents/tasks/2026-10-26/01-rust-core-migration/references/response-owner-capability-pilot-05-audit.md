# Consolidated independent audit: response-owner pilot05 (2026-10-27)

## Decision

The parent accepted Sol's independent audit of the retained pilot05 evidence.
That acceptance is limited to the evidence and its classification. Pilot05 is
still **INCOMPLETE and EXHAUSTED after one actual attempt**; the 68-row
capability, cross-row publisher transition coverage, and memory result were not
accepted as complete. No follow-up grant or run is authorized.

The independent audit report SHA-256 supplied with the parent decision is
`9f26caace693cfb869a3e1f7a38df586af12cd6b33098f18938c1f845af6c2e4`.
The underlying pilot evidence remains at
`/home/ashutosh/.cache/oneshot-capability-pilot-05-20261027/` and is not copied
into the repository.

## Review provenance and accepted evidence

The author ran the single CLI attempt and performed the documented self-checks.
Sol's independent review accepted the retained result and the distinction
between observed evidence and missing coverage. This records an evidence audit;
it does not claim that Sol reran the CLI, repeated child observations, ran the
unreached controls, or reproduced the process-level capture. Previously
accepted scoped reviews retain the individual run-versus-inspection provenance
recorded in their references; this audit does not relabel author pass counts as
reviewer executions.

The accepted evidence establishes six actual owner children and six validated
first observations, three per lane. A seventh journaled slot stopped before
child setup/spawn. There were zero repeated starts and zero controls. The full
60-observation plus eight-control declaration did not complete, and full-grid
reconstruction refused the partial selection. The 21.612-second package
covered provenance, setup, execution, readback, stop, cleanup, and sealing; it
was not a six-row timeout or a native hang.

All 33 accounted owner samples belong to slot 5. They are not cross-row
publisher coverage; earlier short-lived native owners have no retained collector
samples. The six invocation receipts establish distinct child identities, but
they do not fill that sampling gap. Cross-row publisher/collector transition
coverage remains **NOT ESTABLISHED**.

The owner sample that stopped the run retained state R/R, matching identity,
FD enumeration `EACCES`/`PermissionError`, and missing RSS/HWM before reap. No
same-identity exit was established at that sample. The eventual successful
termination does not retroactively prove an exit-teardown exception. The owner
was neither marked accounted nor assigned zero or inferred metrics.

The owned Valkey store and runner process group were confirmed absent. For the
62 pre-existing containers, the compared ID/name/image/status/running/PID/start/
finish/restart fields were unchanged; arbitrary configuration and health logs
were outside that comparison.
Evidence preservation records 149 ledger entries, 34 capture files, 33 capture
seals, 13,420 archived source entries, and four built artifacts. The historical
inventory has 1,670 entries, including the checked original 1,331-entry
history; the prior registry bytes are retained separately from the authorized
replacement.

Memory remains **INCONCLUSIVE**: only one owner lifetime has usable RSS, all
three native lifetimes are unobserved, the first runner sample came about 752 ms
after launch, and some native children lasted about 1.414 ms—shorter than the
requested 5 ms sampling interval. Store memory remains separate from owner RSS.

## Open findings

These are open design or source-review findings, not claims about the cause of
pilot05's owner `EACCES`:

| Priority | Finding | Status and boundary |
| --- | --- | --- |
| P1 | Synchronous full provenance hashing delays passive sampling by about 752 ms; some children are shorter than 5 ms. | **OPEN.** Moving hashing alone does not guarantee observation of short-lived children. |
| P1 | Normal store teardown conflicts with final verification: Go `AttachOwnedValkey` cleanup removes the store before exit, while Python final verification still requires a live store. | **OPEN.** Source-review finding; not established as a pilot05 cause. |
| P2 | Publication, journal, current receipt, discovery, and reaping do not share a coordinated handoff. | **OPEN.** No atomic stop guarantee prevents every next spawn. |
| P2 | Runner/provenance process and descendant traversal retain short-exit races. | **OPEN.** Lifecycle gaps remain; no pilot03/04/05 cause is generalized from this finding. |

Pilot03's original cause remains unproven. Pilot04 remains classified as its
separate-store FD failure. Pilot05 remains the ambiguous owner teardown sample
described above. Do not merge these into a repeated-cause claim.

## Current scoped review state and limits

The parent records the bounded native clock semantics, standalone precedence,
Go artifact/comparator, controls, and resource-contract reviews as accepted at
their declared scopes. That acceptance does not complete capability, memory,
publisher-transition, or performance readiness. Grants01/02 remain closed at
zero attempts; grants03/04/05 each remain spent after one attempt and cannot be
reused.

The source-owned one-shot guards still bind the exact review acceptance,
declaration digest, scope, output, expiry, and one-attempt grant. Python rejects
capture when either the claim or runner marker already exists; the Go direct
entry consumes the runner marker with exclusive creation. The pilot05 report
records both permanent markers retained, so the existing grant is spent and
cannot be reused. This delivery did not edit the registry or capture guards.

There is no comparative, tail-latency, language-advantage, production, steady
state, or transport verdict. FFI_NO_GO and SCREEN_NO_GO remain. Phase 1 is
partial, Phase 2 remains open, and bridge selection is unset. Licensing,
platform, lifecycle, fencing, recovery, activation, rollout, and rollback gates
remain open. No finding in this audit is repaired here.
