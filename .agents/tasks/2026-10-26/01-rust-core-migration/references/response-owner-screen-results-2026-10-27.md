# Fixed response-owner screen result — 2026-10-27

**Parent-accepted SCREEN_NO_GO; stop current FFI-boundary expansion pending architecture assessment.** One parent-authorized screen completed after accepted Luna round 3. Luna's independent result review 01 was accepted by the parent with no blockers: raw evidence was recomputed, the derived result matched byte-for-byte, and 110 external hashes, 81 bundle hashes, 13,358 archived source-manifest entries, and the manifest were verified. All 18 rows validated; the capture had no retries, timeouts, source changes, extra measurement children, or retained inputs. The result makes no architecture, transport, production, or activation selection.

Bound checkout `/home/ashutosh/.t3/worktrees/inngest/t3-a86a57bd`, branch `t3/fixed-screen-mode`, base `0cafa7475e94750da07a3485b78f9f763eef1b7b`. Evidence root: `/home/ashutosh/.cache/response-owner-fixed-screen-20261027-x_q8mo6t`; immutable raw capture: `/home/ashutosh/.cache/response-owner-fixed-screen-20261027-x_q8mo6t/capture`. Old STOP/diagnostic/control bundles are preserved.

Capture 2026-10-27T04:35:46.546589+00:00 to 2026-10-27T04:37:16.583717+00:00: **90.036977s** through controller exit; manifest snapshot before reduction 89.608358s. All setup, preflights, derivation and raw hashing fit the 900s budget, including its 10s reserved shutdown/seal allowance.

## Completed declaration and gates

Seeds 26011/26012/26013; Go/native; controlled; C1; GOMAXPROCS=8; run64/mixed8/run64k. Each of 18 rows has first + 32 warmup + 500 steady slots: **9,594 validated/closed row slots, including 9,000 steady**. One 14-contract functional preflight and six separate one-request port preflights passed; no separate first-observation children. All 25 child command receipts are COMPLETE. Zero timeouts or retained inputs; all 108 lifecycle boundaries exist. Functional error logs are expected contract cases.

Unchanged UNCALIBRATED gates: native/Go resource p95 <=1.10; closed-loop achieved rate >=0.90; owner service rate >=0.90; eligible whole-host RSS <=1.20. Each metric requires a passing median and >=2/3 seeds. For every workload, each latency/rate gate has **0/3 passing seeds**, independently of memory.

Median native/Go ratios (p95; closed-loop rate; owner service rate; observed RSS):
- mixed8: 9.237374547; 0.081031752; 0.074468084; 1.176816591. Screen NO_GO; memory INCONCLUSIVE (descriptive RSS threshold counts 3/3 do not confer eligibility).
- run64k: 23.936382066; 0.044629819; 0.041432515; 1.087515816. Screen NO_GO; memory INCONCLUSIVE (descriptive RSS threshold counts 2/3 do not confer eligibility).
- run64: 9.497520984; 0.113841620; 0.106703668; 1.059600171. Screen NO_GO; memory INCONCLUSIVE (descriptive RSS threshold counts 3/3 do not confer eligibility).

Every per-seed ratio, same order (p95; closed-loop; service; RSS; RSS eligible):
- mixed8/26011: 8.082424716; 0.092876572; 0.086359689; 1.176816591; false.
- mixed8/26012: 10.073283807; 0.081031752; 0.074468084; 1.183500229; false.
- mixed8/26013: 9.237374547; 0.078068683; 0.073045000; 1.037148347; false.
- run64/26011: 9.497520984; 0.113841620; 0.106703668; 1.172131995; true.
- run64/26012: 8.962534558; 0.119607022; 0.112608705; 1.059600171; true.
- run64/26013: 11.167927847; 0.107592057; 0.100633278; 0.902887987; false.
- run64k/26011: 24.901292554; 0.044629819; 0.041432515; 1.297248714; false.
- run64k/26012: 23.936382066; 0.038981974; 0.036618770; 1.041505749; false.
- run64k/26013: 18.866191187; 0.047831108; 0.044980980; 1.087515816; false.

## Raw diagnostics and observer eligibility

Per-row preparation, independent validation/release, dispatch gaps, owner occupancy, p95, both absolute rates, elapsed time, setup and retention are in the rows CSV/JSON; these are steady sums of occupancy, not CPU costs or wall-time subtraction. Dispatch gaps cover 499 adjacent steady transitions per row, excluding warmup transitions. The measured owner includes projection, copies, codecs, allocation and callback drain.

All rows prepared/validated/released exactly 533 inputs; peak live inputs 1, payload capacity 64 or 65,536 bytes; no precreated backend runs. All resource windows and input retention passed completion validation. The observer remains 1ms with a 120,000-record cap. **10/18 rows are RSS SAMPLED, 8/18 INCONCLUSIVE**, all from unchanged density/coverage criteria; only 2/9 memory pairs are conclusive, so every three-seed memory cell remains INCONCLUSIVE. Boundary RSS/offsets are preserved in the 108-row CSV and full JSON.

Periodic counts and maximum steady gaps (eligibility requires >=10 steady points, >=10ms duration and <=5ms edge/inter-sample gaps):
- Row 000 26011/mixed8/native: 1233 periodic, 1156 steady; max gap 12.801369ms; INCONCLUSIVE.
- Row 001 26011/mixed8/go: 119 periodic, 111 steady; max gap 1.204309ms; SAMPLED.
- Row 002 26011/run64k/go: 787 periodic, 735 steady; max gap 3.046533ms; SAMPLED.
- Row 003 26011/run64k/native: 17673 periodic, 16483 steady; max gap 20.320200ms; INCONCLUSIVE.
- Row 004 26011/run64/go: 74 periodic, 69 steady; max gap 1.565476ms; SAMPLED.
- Row 005 26011/run64/native: 672 periodic, 624 steady; max gap 1.456983ms; SAMPLED.
- Row 006 26012/run64/native: 652 periodic, 608 steady; max gap 2.838653ms; SAMPLED.
- Row 007 26012/run64/go: 78 periodic, 73 steady; max gap 1.271274ms; SAMPLED.
- Row 008 26012/mixed8/go: 120 periodic, 113 steady; max gap 1.271394ms; SAMPLED.
- Row 009 26012/mixed8/native: 1401 periodic, 1328 steady; max gap 14.771093ms; INCONCLUSIVE.
- Row 010 26012/run64k/native: 17802 periodic, 16599 steady; max gap 14.612235ms; INCONCLUSIVE.
- Row 011 26012/run64k/go: 713 periodic, 654 steady; max gap 1.972980ms; SAMPLED.
- Row 012 26013/run64k/native: 16432 periodic, 15429 steady; max gap 19.283346ms; INCONCLUSIVE.
- Row 013 26013/run64k/go: 752 periodic, 707 steady; max gap 17.763154ms; INCONCLUSIVE.
- Row 014 26013/run64/go: 73 periodic, 69 steady; max gap 1.390227ms; SAMPLED.
- Row 015 26013/run64/native: 662 periodic, 629 steady; max gap 13.103005ms; INCONCLUSIVE.
- Row 016 26013/mixed8/native: 1373 periodic, 1290 steady; max gap 15.276410ms; INCONCLUSIVE.
- Row 017 26013/mixed8/go: 120 periodic, 112 steady; max gap 1.872652ms; SAMPLED.

## Provenance and reproduction

All 13,358 archived source hashes matched before documentation, and all 81 raw ledger entries verify. The current reviewed reducer reproduced `derived.json` byte-for-byte to external `rederived-v3.json` (exit 0). Independent Luna result review 01 repeated the raw recomputation and byte-identical derivation, verifying the 110 external hashes, 81 bundle hashes, 13,358 source-manifest entries, and manifest; the parent accepted it with no blockers. The accepted Go normal/race 17-test selections and Python 15-method/four-stage real combined controls with each binary were verified from their receipts; they were not rerun as extra measurements. The matching reviewed binary was reused; no build was necessary. Accepted build receipts retain cached Go 1.26.4, cache-backed tmp and GOMAXPROCS=2 / -p=2.

Exact original capture argv/environment/start/exit: `capture-command.json` and `run-once.py` in the evidence root; all individual raw requests, six-boundary snapshots, logs and command receipts remain under `capture/rows/`. `capture/source.tar.gz`, `source.patch`, `source-manifest.json`, `manifest.json` and `artifacts.json` retain source, patch, environment, tools and hashes. The evidence root also stores the exact binary, native archive, review receipts and a separate hash manifest. Authorization was consumed once; the historical capture command is not authorization to repeat it.

Reproduce reduction only with the current reviewed script and a new output path:

```sh
cd /home/ashutosh/.t3/worktrees/inngest/t3-a86a57bd
python3 scripts/response-owner-measure.py derive /home/ashutosh/.cache/response-owner-fixed-screen-20261027-x_q8mo6t/capture \
  --output /home/ashutosh/.cache/response-owner-screen-independent-derived.json
```

SHA-256 bindings:
- `reducer_sha256`: `8774d756e3afc772c26c20bf4a908b06188f2c2348ac7e8f3bfb9cf8d01ce843`.
- `binary_sha256`: `9d1cd95f1dbbe06d79dff2940f3b19b0558dcc41bb0c734d3858c1c2118812a4`.
- `native_static_sha256`: `562139fab7fec3ed44cdcb596276c1241d038ed95c0d34a165fa7d6d0f28ed7d`.
- `manifest_sha256`: `eeaff54126ce2860a360caa310e6c77614f0a72d3d13c0c418e7a5bfc25b4225`.
- `raw_artifact_ledger_sha256`: `51e0c35830189c5a62843587c4a9a61c8ccc5efc394fb67979c771b74ec2abaa`.
- `source_manifest_sha256`: `62c5729d8102817cd9f7845653df61abae7eea1b064b798bf2bdb3046d275398`.
- `source_archive_sha256`: `ca33fcdcc903a1ccd8e191af612754b2d6bc4d059920552ad826bcf08abd7212`.
- `source_patch_sha256`: `e25174d65b0afb4134f865d84b449ac4f85b1694cb83f72c549d9f99c619b42a`.
- `derived_sha256`: `106a1998e7a76db1474aa6f61c0e537e6b33dbf439c8eb60a9a1eb0e831ef3ea`.
- `review_manifest_sha256`: `895273923d83fbfb9fdc207594e95c24dd6833c4a93a9fd4812f4c9aa48a5ad2`.

A post-capture hash verifier initially used `hashlib.file_digest`, unavailable in Python 3.10; it failed before verification. The corrected external verifier used streamed SHA-256 and passed. The failure is retained in `verification-initial-error.txt`; capture was not repeated.

## Host limits and next gate

AMD Ryzen 9 5950X, affinity 0-31, unpinned. Capture load averages: before `3.06 4.27 4.78 8/10629 1767606`, after `3.93 4.23 4.72 2/10377 1772601`. The visible user.slice ancestor reports `cpu.max=max 100000`, cpuset 0-31; process cgroup/other ancestor paths were unavailable, so effective CPU quota is unknown. Raw host/cgroup errors are retained, and no CPU-consumption verdict is made. Background services were untouched; no parallel measurement process was found before capture.

- Controlled/C1/GOMAXPROCS=8 only; C8, Valkey, retryable and enqueue-error workloads unmeasured. Functional error-contract tests do not provide error-workload performance coverage.
- Unpinned shared AMD Ryzen 9 5950X host; affinity 0-31; background services were untouched. Effective process cgroup quota is unknown: leaf paths unavailable; visible user.slice cpu.max is max 100000.
- RSS is whole host-process memory, with no precise Go/Rust attribution. All six lifecycle boundaries exist for every row, but 8/18 rows fail unchanged RSS density/coverage eligibility.
- Owner service rate is inverse mean wall occupancy, not CPU consumption. Observer/provenance/validation/preparation costs are retained under the accepted protocol.
- UNCALIBRATED thresholds; fixed rejection screen only. No overall PASS, production/transport selection, activation, full-matrix extrapolation or general Rust infeasibility conclusion.
- Persistent-process comparison, native arm64/final Cgo link, checkpoint/late-CritT/Stop/fencing/uncertain-or-lost-ack/lifecycle/recovery, calibration and activation gates remain open.
- STOP snapshots on failures are best effort: admitted in-flight work may retain inputs or an unfinished slot; no failure/drain guarantee is inferred from this successful capture.

Stop current FFI-boundary expansion; the next architecture assessment is pending. No additional capture, profile, optimization, or process implementation follows from this SCREEN_NO_GO. Broad Phase 1 remains partial, Phase 2 open, bridge unset, and production activation absent. No CPU-consumption, production-performance, transport-choice, or broad Rust-infeasibility conclusion follows from this controlled C1 screen on a shared unpinned host with unknown effective quota and uncalibrated thresholds.

Artifacts: [machine-readable summary](response-owner-screen-results-2026-10-27.json), [per-seed ratios](response-owner-screen-results-2026-10-27-pairs.csv), [per-row diagnostics](response-owner-screen-results-2026-10-27-rows.csv), [all RSS boundaries](response-owner-screen-results-2026-10-27-rss-boundaries.csv).
