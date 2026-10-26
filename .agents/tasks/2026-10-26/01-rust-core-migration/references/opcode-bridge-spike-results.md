# Opcode bridge spike amd64 measurement results

**State:** campaign complete on Linux/amd64; Phase 2 has amd64 measurements only; bridge choice unset.
**Review:** the parent accepted correctness, including the FFI initialized-output fix, bounded lifecycle/restart behavior, shutdown race fix, and measurement fairness corrections. Luna accepted the harness review. Sol independently verified all 21 command results, 288 rows across three seeds, 180 serial benchmark repetitions, source/raw hashes, and calculations; no blockers were found. The accepted P2 startup-order interpretation limit is recorded below.

## Scope and method

Executed the exact [campaign command file](/home/ashutosh/.cache/go-tmp-inngest-opcode-spike/measurement-capabilities/campaign-commands.txt) (SHA-256 `5107cc40543ef3f7a1b36a7a7bbac8c8a8efb652e5ea0c0ce949019b502f43ba`) in the specified worktree. Three seeded latency runs (75501, 75502, 75503) and 18 serial benchmark commands completed sequentially in 11.3 minutes; all 21 commands exited 0.

Each latency run contains 96 unique lane/workload/concurrency rows (six lanes × eight workloads × concurrency 1 and 8) with 10,000 individual request-duration samples per row. The harness reports nearest-rank p50/p95/p99 (`ceil(p*N)-1`) from those actual samples. Closed-loop throughput is the actual number of completed calls divided by steady-state wall time at the row concurrency. Startup and first-request distributions are separate from steady-state measurements.

Each candidate row is paired with its Go helper for the same seed, workload, and concurrency. The threshold status is based on the median of the three paired run ratios; the tables also show how many individual runs met each threshold. `medianRunP95Nanoseconds` in the JSON is the median of the three per-run p95 summaries, not a p95 pooled across runs. The latency CSV preserves each run’s p50/p95/p99 values.

Benchmarks are serial with a size-one process pool. Each of the 18 commands reports ten one-second Go benchmark repetitions. Derived serial operations/second is `1e9 / arithmetic mean(ns/op)`; it is not concurrent throughput. `B/op` and `allocs/op` are serial benchmark means. Their 0.90 ratio status is informational and does not replace the concurrency 1/8 closed-loop throughput gate.

Go benchmark `B/op` and `allocs/op` do not include allocations made inside the Rust allocator. The committed artifacts contain row summaries, resource observations, benchmark repetitions, command details, and raw hashes, but not each latency row’s 10,000 individual request durations. The reported percentiles therefore cannot be independently reconstructed from the committed artifacts; raw logs remain in the local cache outside the repository.

## Environment and fingerprints

| Item | Value |
| --- | --- |
| Host | Linux x86_64, AMD Ryzen 9 5950X, 16 cores / 32 logical CPUs |
| Affinity / benchmark parallelism | CPUs 0–31; Go benchmark suffix `-32`; campaign commands were sequential and did not override `GOMAXPROCS` |
| Preflight load snapshot | 2026-10-26 20:31:58 UTC: load average 4.44 / 13.90 / 14.19 (1 / 5 / 15 min); no CPU pinning |
| Toolchains | Go 1.26.4, Rust 1.94.0 |
| Go measurement binary | SHA-256 `abfb071d7e2d7b4f44f2ba94c7280abe5f0d1db90d1825c346ed24844c123f69`; tags `opcode_bridge_spike,opcode_bridge_ffi`; Cgo enabled; current before campaign; no rebuild |
| Rust amd64 worker | SHA-256 `ee0a3445b7ab90dbdc57e4bb1f375db087075134208e98ea113d4c80b87acb47` |
| Rust amd64 FFI archive | SHA-256 `0fda795af636942e04e7f2be0f9988cdb7586b0921882f45ffcb5e81f66c5abe` |
| Go source input manifest | b1c7d7677299b5187b812c6b9d58b5cbf96f4593ba3ebc3458eabdd21424f9f3 (`53` files) |
| Rust source input manifest | e3b2997db49376707567b8201467bf3698c5da5af4996a23d7d9bd85aba57bc7 (`9` files) |
| Go temporary directory | `/home/ashutosh/.cache/go-tmp-inngest-opcode-spike` |

## Latency and resource threshold outcomes

The proposed thresholds remain uncalibrated and are applied unchanged: p95 ratio ≤ 1.10, closed-loop throughput ratio ≥ 0.90, aggregate sampled peak RSS ratio ≤ 1.20. Each ratio is candidate / matched Go helper. For memory, Go and FFI use host RSS; process uses host plus that row’s live worker RSS.

| Lane | p95 pass | Closed-loop throughput pass | Peak RSS pass | All three pass |
| --- | ---: | ---: | ---: | ---: |
| `process_decoded` | 1/16 | 1/16 | 9/16 | 1/16 |
| `process_full_driver_response_json` | 1/16 | 0/16 | 16/16 | 0/16 |
| `ffi_decoded` | 2/16 | 2/16 | 16/16 | 1/16 |
| `ffi_full_driver_response_json` | 11/16 | 10/16 | 16/16 | 8/16 |

The decoded process path shows substantial overhead against these targets: only 1/16 workload/concurrency cells pass all three thresholds. The decoded FFI path passes all three in 1/16 cells. The full-response process path passes none of 16 cells; the full-response FFI path passes all three in 8/16. These are reported outcomes; no thresholds were relaxed.

For a concrete serial example, `small1` measured 150.0 ns/op for the Go decoded helper, 18,103.8 ns/op for decoded process, and 504.8 ns/op for decoded FFI. `mixed8` measured 505.4 ns/op for Go and 871.8 ns/op for FFI. These serial observations describe the narrow harness paths and make no production performance claim.

Only 2/16 full-response FFI cells pass all three metrics on every run. The median-based 8/16 result is not a robust bridge-selection signal. The host was unpinned, the preflight load was elevated (4.44 / 13.90 / 14.19 for the 1 / 5 / 15 minute averages), and the three-run variation limits conclusions from those medians. These results remain a narrow negative for decoded FFI/process and an incomplete result for bridge selection.

Each table cell is `median paired ratio`, `PASS`/`FAIL`, and individual-run pass count.

### `process_decoded` vs `go_helper_decoded`

| Workload | C1 p95 | C1 throughput | C1 peak RSS | C8 p95 | C8 throughput | C8 peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `duplicates` | 62.466 FAIL (0/3) | 0.020 FAIL (0/3) | 1.123 PASS (3/3) | 86.127 FAIL (0/3) | 0.057 FAIL (0/3) | 1.309 FAIL (0/3) |
| `empty` | 196.802 FAIL (0/3) | 0.010 FAIL (0/3) | 1.117 PASS (3/3) | 321.762 FAIL (0/3) | 0.065 FAIL (0/3) | 1.302 FAIL (0/3) |
| `large1000` | 1.302 FAIL (0/3) | 0.410 FAIL (0/3) | 0.851 PASS (3/3) | 0.877 PASS (3/3) | 1.608 PASS (2/3) | 0.922 PASS (3/3) |
| `lazy` | 151.935 FAIL (0/3) | 0.011 FAIL (0/3) | 1.149 PASS (3/3) | 132.531 FAIL (0/3) | 0.064 FAIL (0/3) | 1.282 FAIL (0/3) |
| `mixed8` | 42.294 FAIL (0/3) | 0.027 FAIL (0/3) | 1.103 PASS (3/3) | 45.416 FAIL (0/3) | 0.055 FAIL (0/3) | 1.238 FAIL (0/3) |
| `opaque_payload` | 87.329 FAIL (0/3) | 0.015 FAIL (0/3) | 1.125 PASS (3/3) | 144.187 FAIL (0/3) | 0.074 FAIL (0/3) | 1.294 FAIL (0/3) |
| `priority` | 66.126 FAIL (0/3) | 0.018 FAIL (0/3) | 1.142 PASS (3/3) | 108.325 FAIL (0/3) | 0.055 FAIL (0/3) | 1.298 FAIL (0/3) |
| `small1` | 81.112 FAIL (0/3) | 0.016 FAIL (0/3) | 1.156 PASS (3/3) | 113.316 FAIL (0/3) | 0.054 FAIL (0/3) | 1.287 FAIL (0/3) |

### `process_full_driver_response_json` vs `go_helper_full_driver_response_json`

| Workload | C1 p95 | C1 throughput | C1 peak RSS | C8 p95 | C8 throughput | C8 peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `duplicates` | 2.526 FAIL (0/3) | 0.323 FAIL (0/3) | 1.041 PASS (3/3) | 2.473 FAIL (0/3) | 0.249 FAIL (0/3) | 1.127 PASS (3/3) |
| `empty` | 4.818 FAIL (0/3) | 0.121 FAIL (0/3) | 1.035 PASS (3/3) | 5.377 FAIL (0/3) | 0.115 FAIL (0/3) | 1.181 PASS (2/3) |
| `large1000` | 1.065 PASS (3/3) | 0.875 FAIL (1/3) | 1.025 PASS (3/3) | 1.361 FAIL (0/3) | 0.850 FAIL (0/3) | 1.142 PASS (3/3) |
| `lazy` | 3.779 FAIL (0/3) | 0.205 FAIL (0/3) | 1.052 PASS (3/3) | 3.090 FAIL (0/3) | 0.210 FAIL (0/3) | 1.156 PASS (2/3) |
| `mixed8` | 2.054 FAIL (0/3) | 0.380 FAIL (0/3) | 1.048 PASS (3/3) | 2.129 FAIL (0/3) | 0.291 FAIL (0/3) | 1.146 PASS (2/3) |
| `opaque_payload` | 3.280 FAIL (0/3) | 0.315 FAIL (0/3) | 1.050 PASS (3/3) | 2.945 FAIL (0/3) | 0.212 FAIL (1/3) | 1.117 PASS (3/3) |
| `priority` | 3.987 FAIL (0/3) | 0.305 FAIL (0/3) | 1.051 PASS (3/3) | 2.180 FAIL (0/3) | 0.298 FAIL (0/3) | 1.142 PASS (3/3) |
| `small1` | 4.138 FAIL (0/3) | 0.190 FAIL (0/3) | 1.041 PASS (3/3) | 5.250 FAIL (0/3) | 0.111 FAIL (0/3) | 1.168 PASS (2/3) |

### `ffi_decoded` vs `go_helper_decoded`

| Workload | C1 p95 | C1 throughput | C1 peak RSS | C8 p95 | C8 throughput | C8 peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `duplicates` | 2.200 FAIL (0/3) | 0.438 FAIL (0/3) | 1.011 PASS (3/3) | 2.603 FAIL (0/3) | 0.750 FAIL (1/3) | 1.019 PASS (3/3) |
| `empty` | 3.079 FAIL (0/3) | 0.464 FAIL (0/3) | 1.028 PASS (3/3) | 6.323 FAIL (0/3) | 0.751 FAIL (1/3) | 0.999 PASS (3/3) |
| `large1000` | 0.817 PASS (3/3) | 0.777 FAIL (1/3) | 0.928 PASS (3/3) | 0.796 PASS (3/3) | 1.286 PASS (3/3) | 0.921 PASS (3/3) |
| `lazy` | 4.008 FAIL (0/3) | 0.342 FAIL (0/3) | 1.023 PASS (3/3) | 3.865 FAIL (0/3) | 0.773 FAIL (0/3) | 0.993 PASS (3/3) |
| `mixed8` | 2.000 FAIL (0/3) | 0.541 FAIL (0/3) | 0.995 PASS (3/3) | 2.058 FAIL (0/3) | 0.903 PASS (2/3) | 1.009 PASS (3/3) |
| `opaque_payload` | 3.200 FAIL (0/3) | 0.323 FAIL (0/3) | 1.007 PASS (3/3) | 3.122 FAIL (0/3) | 0.862 FAIL (1/3) | 1.017 PASS (3/3) |
| `priority` | 2.661 FAIL (0/3) | 0.321 FAIL (0/3) | 1.023 PASS (3/3) | 3.520 FAIL (0/3) | 0.839 FAIL (0/3) | 1.052 PASS (3/3) |
| `small1` | 2.369 FAIL (0/3) | 0.461 FAIL (0/3) | 1.026 PASS (3/3) | 2.680 FAIL (0/3) | 0.803 FAIL (0/3) | 1.014 PASS (3/3) |

### `ffi_full_driver_response_json` vs `go_helper_full_driver_response_json`

| Workload | C1 p95 | C1 throughput | C1 peak RSS | C8 p95 | C8 throughput | C8 peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `duplicates` | 1.125 FAIL (1/3) | 0.975 PASS (2/3) | 1.058 PASS (3/3) | 1.058 PASS (2/3) | 0.893 FAIL (1/3) | 1.019 PASS (3/3) |
| `empty` | 1.047 PASS (3/3) | 1.012 PASS (2/3) | 0.994 PASS (3/3) | 1.017 PASS (2/3) | 1.200 PASS (3/3) | 1.011 PASS (3/3) |
| `large1000` | 1.012 PASS (3/3) | 1.003 PASS (3/3) | 1.008 PASS (3/3) | 0.939 PASS (2/3) | 1.072 PASS (3/3) | 0.851 PASS (3/3) |
| `lazy` | 1.205 FAIL (1/3) | 0.772 FAIL (1/3) | 1.015 PASS (3/3) | 1.093 PASS (2/3) | 0.963 PASS (2/3) | 1.007 PASS (3/3) |
| `mixed8` | 1.007 PASS (2/3) | 0.902 PASS (2/3) | 1.025 PASS (3/3) | 1.161 FAIL (1/3) | 0.875 FAIL (1/3) | 1.007 PASS (3/3) |
| `opaque_payload` | 0.961 PASS (2/3) | 1.405 PASS (2/3) | 1.000 PASS (3/3) | 0.960 PASS (3/3) | 1.075 PASS (3/3) | 0.987 PASS (3/3) |
| `priority` | 1.414 FAIL (1/3) | 0.892 FAIL (1/3) | 1.023 PASS (3/3) | 1.028 PASS (3/3) | 0.449 FAIL (1/3) | 1.029 PASS (3/3) |
| `small1` | 1.026 PASS (2/3) | 0.835 FAIL (1/3) | 1.006 PASS (3/3) | 1.105 FAIL (1/3) | 0.952 PASS (3/3) | 0.994 PASS (3/3) |

## Serial benchmark means and allocations

All benchmark rows are serial. Derived operations/second is the inverse of mean ns/op. Ratios compare to the corresponding Go helper for the same decoded or full-response JSON route and workload. The benchmark ratio column is informational; the primary throughput threshold is the measured closed-loop ratio above.

| Lane | Workload | Mean ns/op | Ten-run range ns/op | Derived ops/s | Mean B/op | Mean allocs/op | Serial throughput ratio vs Go | 0.90 status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `go_helper_decoded` | `small1` | 150.0 | 135.0–165.3 | 6665777.9 | 24.0 | 2.0 | 1.000 | BASELINE |
| `go_helper_decoded` | `mixed8` | 505.4 | 443.1–538.8 | 1978669.9 | 368.0 | 9.0 | 1.000 | BASELINE |
| `go_helper_decoded` | `large1000` | 28616.5 | 22879.0–36967.0 | 34944.9 | 53808.0 | 29.0 | 1.000 | BASELINE |
| `go_helper_full_driver_response_json` | `small1` | 5971.0 | 5315.0–6882.0 | 167476.1 | 1280.0 | 22.0 | 1.000 | BASELINE |
| `go_helper_full_driver_response_json` | `mixed8` | 21950.8 | 20700.0–24874.0 | 45556.4 | 3992.0 | 65.0 | 1.000 | BASELINE |
| `go_helper_full_driver_response_json` | `large1000` | 2776211.2 | 2346832.0–3501868.0 | 360.2 | 417595.6 | 7052.0 | 1.000 | BASELINE |
| `process_decoded` | `small1` | 18103.8 | 15710.0–21605.0 | 55237.0 | 1452.0 | 24.0 | 0.008 | FAIL |
| `process_decoded` | `mixed8` | 18388.8 | 15218.0–22664.0 | 54380.9 | 1720.0 | 26.0 | 0.027 | FAIL |
| `process_decoded` | `large1000` | 82041.4 | 69013.0–103144.0 | 12189.0 | 47416.7 | 29.0 | 0.349 | FAIL |
| `process_full_driver_response_json` | `small1` | 30658.4 | 27309.0–35358.0 | 32617.5 | 2704.0 | 44.0 | 0.195 | FAIL |
| `process_full_driver_response_json` | `mixed8` | 56144.5 | 46955.0–64260.0 | 17811.2 | 5344.0 | 82.0 | 0.391 | FAIL |
| `process_full_driver_response_json` | `large1000` | 2914194.7 | 2593135.0–3265450.0 | 343.1 | 411216.0 | 7052.0 | 0.953 | PASS |
| `ffi_decoded` | `small1` | 504.8 | 488.5–530.4 | 1980825.6 | 120.0 | 8.0 | 0.297 | FAIL |
| `ffi_decoded` | `mixed8` | 871.8 | 742.8–973.7 | 1147104.7 | 407.8 | 10.0 | 0.580 | FAIL |
| `ffi_decoded` | `large1000` | 40386.6 | 34245.0–45087.0 | 24760.7 | 46084.1 | 13.0 | 0.709 | FAIL |
| `ffi_full_driver_response_json` | `small1` | 5555.2 | 5021.0–5815.0 | 180011.5 | 1376.0 | 28.0 | 1.075 | PASS |
| `ffi_full_driver_response_json` | `mixed8` | 28726.4 | 19709.0–47642.0 | 34811.2 | 4032.0 | 66.0 | 0.764 | FAIL |
| `ffi_full_driver_response_json` | `large1000` | 3704454.2 | 2327996.0–5000113.0 | 269.9 | 409890.9 | 7036.0 | 0.749 | FAIL |

## Process startup and first-request observations

Each latency run emits six process-startup groups in the same fixed order before steady-state rows are shuffled. The accepted P2 limitation is that startup groups were not randomized; these are ordered observations, not randomized startup comparisons. No cold-cache startup comparison was controlled. No FFI initialization duration is collected as a process-startup measurement; FFI rows have no spawn/READY duration. Each startup group has 500 samples and no RSS sampling; the CSV and JSON retain p50/p95/p99 by run.

| Group order | Process lane | Workload | READY p95 ms (75501 / 75502 / 75503) | First completed request p95 ms (75501 / 75502 / 75503) |
| ---: | --- | --- | --- | --- |
| 1 | `process_decoded` | `small1` | 0.617 / 0.690 / 0.577 | 0.040 / 0.043 / 0.036 |
| 2 | `process_decoded` | `mixed8` | 0.651 / 0.678 / 0.602 | 0.041 / 0.041 / 0.036 |
| 3 | `process_decoded` | `large1000` | 0.661 / 0.715 / 0.649 | 0.111 / 0.122 / 0.099 |
| 4 | `process_full_driver_response_json` | `small1` | 0.669 / 0.726 / 0.646 | 0.059 / 0.061 / 0.058 |
| 5 | `process_full_driver_response_json` | `mixed8` | 0.615 / 0.708 / 0.607 | 0.081 / 0.096 / 0.074 |
| 6 | `process_full_driver_response_json` | `large1000` | 0.859 / 0.970 / 0.881 | 3.386 / 3.687 / 3.895 |

## Resource observations

All 288 rows have `baseline`, `ready`, `warmup`, `end`, and `after_close` snapshots. All 96 process rows retain worker PIDs through the measurement and record every worker as `absent_expected_after_close`. Host FD and Go goroutine counts are at or below the pre-pool baseline after close. Thread counts and RSS are reported at the collected stages and may remain retained; exited worker counters are omitted. Go and FFI rows have host-only scope; process rows use host plus live worker RSS. Startup-group RSS is not sampled.

Peak RSS uses the largest host-plus-live-worker sum in one 10 ms observation, not a sum of independent process maxima. `/proc` counters are read sequentially; sampling can miss short peaks, summed process RSS double-counts shared pages, and host RSS includes the Go test runtime and workload fixtures. These memory ratios are not calibrated allocator measurements.

## Exact command statuses

The following 21 statuses are in the command file’s order. The structured results JSON contains every exact command string, start/end time, exit code, duration, raw log path, and full SHA-256. Raw logs remain under `/home/ashutosh/.cache/go-tmp-inngest-opcode-spike/measurement-capabilities`.

| # | Output/log | Exit | Wall seconds | Raw log SHA-256 prefix |
| ---: | --- | ---: | ---: | --- |
| 1 | `latency-75501.log` | 0 | 117.681 | `b66c915702222b3b` |
| 2 | `latency-75502.log` | 0 | 126.461 | `0ae37faed89084ce` |
| 3 | `latency-75503.log` | 0 | 119.449 | `f2c9a86cfd333ac3` |
| 4 | `bench-go_helper_decoded-small1.log` | 0 | 17.247 | `9b4ba989fd55fe49` |
| 5 | `bench-go_helper_decoded-mixed8.log` | 0 | 20.215 | `077c3000b0001df5` |
| 6 | `bench-go_helper_decoded-large1000.log` | 0 | 17.654 | `e823a9e3d6a40399` |
| 7 | `bench-go_helper_full_driver_response_json-small1.log` | 0 | 13.976 | `b0fc6eb9f4f86e2a` |
| 8 | `bench-go_helper_full_driver_response_json-mixed8.log` | 0 | 14.318 | `304c6f20d1152239` |
| 9 | `bench-go_helper_full_driver_response_json-large1000.log` | 0 | 15.616 | `dcda8625b3a6f914` |
| 10 | `bench-process_decoded-small1.log` | 0 | 16.045 | `d4ca7b18d562a297` |
| 11 | `bench-process_decoded-mixed8.log` | 0 | 17.495 | `501bf4c2baa37aef` |
| 12 | `bench-process_decoded-large1000.log` | 0 | 22.008 | `164bfe2e6e28eb43` |
| 13 | `bench-process_full_driver_response_json-small1.log` | 0 | 18.079 | `1b45afa748bc26a6` |
| 14 | `bench-process_full_driver_response_json-mixed8.log` | 0 | 20.534 | `2798f7d43d011ece` |
| 15 | `bench-process_full_driver_response_json-large1000.log` | 0 | 15.206 | `2ad23b5d4bca438b` |
| 16 | `bench-ffi_decoded-small1.log` | 0 | 17.312 | `49922dbfd54588dd` |
| 17 | `bench-ffi_decoded-mixed8.log` | 0 | 22.915 | `b898db5f153af227` |
| 18 | `bench-ffi_decoded-large1000.log` | 0 | 16.935 | `2465f1bbf234412c` |
| 19 | `bench-ffi_full_driver_response_json-small1.log` | 0 | 13.721 | `97683a49088de21a` |
| 20 | `bench-ffi_full_driver_response_json-mixed8.log` | 0 | 17.602 | `1d9c983594a190f2` |
| 21 | `bench-ffi_full_driver_response_json-large1000.log` | 0 | 17.981 | `c57aca1cbd42342e` |

## Remaining gates and limits

- Broad Phase 1 remains partial; this measurement evidence does not complete the Go reference baseline.
- Phase 2 has Linux/amd64 measurement evidence only. Native Linux/arm64 process execution and the final GNU Cgo link/runtime check remain open; no arm64 performance claim is made.
- FFI hard cancellation is unavailable. `CGO_ENABLED=0` process packaging versus the FFI policy remains unresolved.
- The bridge remains unselected. This campaign makes no architecture decision and provides no production handoff.
- If the spike continues, the next bounded experiment is a direct FFI caller-buffer path that avoids the wire-frame round trip. It is exploratory and commits to no bridge choice.

## Artifacts

- [Structured results JSON](opcode-bridge-spike-results.json) — 288 latency rows and resource series, paired comparisons, ordered startup observations, 180 serial benchmark repetitions, command statuses and full raw-file hashes.
- [Latency rows CSV](opcode-bridge-spike-latency.csv) — 288 per-run lane/workload/concurrency rows with p50/p95/p99, ratios and gate outcomes.
- [Startup observations CSV](opcode-bridge-spike-startup.csv) — 18 fixed-order startup groups with p50/p95/p99.
- [Serial benchmark CSV](opcode-bridge-spike-benchmarks.csv) — 18 means and allocations plus all ten ns/op, B/op, and allocs/op repetitions per benchmark.
- Raw logs and three raw schema-v3 latency JSON files remain in the cache directory named above; their full hashes are recorded in the structured results JSON.

The durable structured JSON is 1,863,550 bytes (about 1.78 MiB); the three CSV files total 78,150 bytes (about 76.3 KiB). The JSON retains the resource and command audit without copying the 10,000 individual latency durations per row into the repository.
