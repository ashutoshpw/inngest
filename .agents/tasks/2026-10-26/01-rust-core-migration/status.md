# Status

- **State:** in-progress
- **Updated:** 2026-10-26

## Notes

- Work package 1 remains incomplete, with two accepted baseline increments. Sol authored the SDK request wire/replay-shaped input corpus; Luna independently reviewed it with no blockers. The malformed-case and benchmark reset-overhead-label findings are fixed. The corpus pins `MarshalV1` request bytes and request-side replay inputs; it does not cover full SDK-response replay. See the [corpus README](../../../../pkg/execution/driver/testdata/sdk-request-v1/README.md).
- Opcode-plan and `ParseGenerator` fixtures are accepted after Sol's independent high-effort review found no blockers. The initial review's three P2 findings were fixed. Review covered ordering and failure behavior, production gate negative cases, isolated coverage for all seven plan-capable opcode families, focused/race results, and unchanged runtime source. Exact commands, results, mutation probes, and benchmark limits are in the [opcode baseline](references/opcode-plan-baseline.md).
- The focused opcode tests, executor and HTTP parser package tests, focused race run, selected defer/finalization/coalescing lifecycle tests, and opcode-plan benchmarks passed with Go 1.26.4. These recorded suites are not being rerun for this delivery because no runtime or test code changed.
- Temporary executor mutations for SavePending ordering/failure, the plan-capability gate, and the non-lazy-count guard each failed on their intended assertions; `executor.go` was restored to its recorded SHA-256 after every probe. These checks and their evidence are documented in the opcode baseline.
- Remaining phase 1 work includes full SDK-response replay, CEL and SQLite/Postgres SQL parity, queue/state persistence, broader recovery and lifecycle/fault coverage, matched performance baselines, and owner confirmation of supported SDK versions. The [SDK/deployment matrix](references/sdk-deployment-matrix.md) records Linux amd64/arm64 as the first bridge targets, macOS/Windows as deferred parity gates before broader release or Go retirement, and cloud parity as a separate gate.
- Next stacked implementation layers after the opcode slice are CEL/SQL parity, then persistence/recovery; the [main plan](plan.md) remains the concise phase map.
- First-layer delivery is open for review in [PR #1](https://github.com/ashutoshpw/inngest/pull/1), targeting `main` from `rust-core/01-compatibility-baseline`. The PR-opening head was `b480c778c22e279e5212637b585c7d10e22612e6`; accepted-baseline review is complete, and the PR has not been merged. The immediate GitHub snapshot reported no check runs yet.
