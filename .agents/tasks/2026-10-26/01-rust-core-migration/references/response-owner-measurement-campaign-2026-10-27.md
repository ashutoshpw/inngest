# Response-owner campaign attempt — stopped

**Capture date:** 2026-10-27

**Status:** STOPPED — incomplete capture; performance gates not derived

**Review:** Parent accepted independent Sol stop audit; only the separately bounded [one-row diagnostic](response-owner-diagnostic-2026-10-27.md) is authorized

The parent authorized one full declared campaign attempt after accepting the harness reviews. This attempt stopped at its first steady child under the existing watchdog. It is not a threshold verdict and does not select a bridge.

## Declared matrix and preflights

The reviewed selector produced 480 child declarations: 180 steady rows for seeds 26011, 26012, and 26013, totaling 360,000 steady requests, plus 300 separate first-observation children. The matrix covered Go and native lanes, controlled and both real-Valkey queue profiles, five workloads, and C1/C8. Controlled rows declared 5,000 requests; Valkey rows declared 500; each steady worker had 32 warmups.

The campaign's functional preflight passed. All 30 lane/profile/workload port preflights passed, including both real-Valkey queue modes. These are setup/port checks; they do not replace the 180 steady rows or 300 separate first-observation children.

## Stop condition

The first selected steady row was seed 26011, native lane, controlled profile, run64k workload, C1, with 5,000 steady requests and 32 warmups per slot. The child planned 5,033 samples including the first observation.

The internal 110-second watchdog stopped the row. It retained 3,419 complete samples: the first observation, all 32 warmups, and 3,386 steady requests. Request ID 3419, the next steady request, was in flight when the row watchdog expired; request timeout/native hang not established. The original raw `result_class="stuck"` and `timeout=true` fields remain preserved exactly, but describe the old watchdog classification rather than a proven request deadline or native hang. The accepted Sol audit found completions at 109.919/109.954/109.990 seconds, with the next request in flight for less than 48 ms at expiry: sustained progress through the row budget. The row process exited 2, and the capture exited 1 after 117.38 seconds in the outer test. No later steady row or separate first-observation child ran.

The partial row has only baseline and ready resource snapshots; its input-retention counters are watchdog-start values, not a completed lifecycle. It lacks completed delivery/resource/slot-close evidence. The reducer did not create derived output. No p95, closed-loop throughput, owner service-rate, memory ratio, per-cell verdict, or overall campaign verdict is available. The unchanged uncalibrated gates remain p95 <= 1.10, closed-loop throughput >= 0.90, owner service rate >= 0.90, and sampled peak host RSS <= 1.20, with a median pass and at least two of three seeds passing each metric in every cell. None were evaluated.

No request count, deadline, watchdog, selector, or harness source was changed, and no row was rerun. The evidence supports stopping this campaign under its declared protocol. It does not establish relative owner performance or justify bridge selection.

## Environment and Valkey preflight observations

The host had 32 logical CPUs available on CPU IDs 0–31, with GOMAXPROCS=8 in measurement children. The pre-capture load average was 14.12 / 6.56 / 6.13 and MemAvailable was about 36.14 GB. The first row recorded load 5.99 / 5.67 / 5.85 and CPU availability 0–31. Per-row load and CPU availability remain in the raw files; no host affinity or resource budget was changed.

The dedicated Valkey server was version 8.0.3, image ID sha256:88251207ac13df6d4bd4d3a77e6953ca3b90e0e5cc6acb6b3ece0fc2f9091289, repo digest valkey/valkey@sha256:4e78d8ccab4d8978afca140a3dd75b4e45d683d1e51602ee71e976bcc2bd939e, bound at 127.0.0.1:32770. The 10 Valkey-off preflights had median PING latency 75,021 ns; the 10 Valkey-on preflights had median 77,090 ns. Their end-stage INFO memory medians were 2,117,320 bytes and 1,900,868 bytes, respectively. These are first-observation preflight checks only. The disposable container was removed; the 14 preexisting running containers were unchanged.

## Evidence and reproduction

Raw bundle root: /home/ashutosh/.cache/response-owner-review-20261027-jSVKc2

Campaign capture: /home/ashutosh/.cache/response-owner-review-20261027-jSVKc2/campaign

Dry declaration: /home/ashutosh/.cache/response-owner-review-20261027-jSVKc2/plan

Machine-readable audit: response-owner-measurement-campaign-2026-10-27.json

The campaign artifact ledger contains 101 files with zero hash mismatches. The plan ledger contains six files with zero mismatches. The root bundle manifest records 117 files; its SHA-256 is d96c3044c0131766203d887ade3799712d2cb003a19f3d66007f3bb49204ae08. The campaign source archive SHA-256 is 12baba13bf8ba4b8735c59b3a1b119c54d5bbf437e1628002ef6eadac5986dcd. The test binary remained 4124474f485d544e3a4cd9e11350009770387832628b80cdcd9dcfd78c0ae678; the native archive remained 562139fab7fec3ed44cdcb596276c1241d038ed95c0d34a165fa7d6d0f28ed7d. The capture manifest reports no source changes during execution and no binary or native archive changes. A post-capture comparison matched all 9,948 code and lock files to the captured manifest with no added, changed, or removed code; only the task plan/status/protocol and these two report files changed afterward. The binary and native archive still match their captured hashes. See post-capture-verification.json in the raw bundle. Reducer SHA-256: 118ce309cf80dd5f4fab202d719a003466db3a01af1d4c926cad3db64ca71310. Reducer test SHA-256: 108897adae6ee0aec3103f3d3b2d105edc4b56fde8ebe6c494661372310297ef.

The exact capture command and child commands are in campaign/manifest.json and campaign/rows/*-command.json. Re-run the raw audit with; it writes only the audit JSON/log at the bundle root and leaves the sealed plan and campaign directories untouched:

    python3 /home/ashutosh/.cache/response-owner-review-20261027-jSVKc2/audit-aborted-campaign.py

The parent later accepted an estimated 161.9–167.4-second diagnostic budget need and authorized exactly one replay with explicit coordinated capture budgets. This estimate is diagnostic only, with no performance conclusion. The [diagnostic reference](response-owner-diagnostic-2026-10-27.md) records that new authorization separately; it does not resume the campaign or modify this failed bundle.

Phase 1 remains partial. Phase 2 remains open. Bridge choice remains unset. Native arm64, persistent-process comparison, and the existing lifecycle and rollout gates remain open.
