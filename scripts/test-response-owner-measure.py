#!/usr/bin/env python3
"""Reducer regression: synthetic RSS tests eligibility, never OS memory behavior."""

import argparse
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import sys
import unittest

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "measure", Path(__file__).with_name("response-owner-measure.py")
)
measure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure)


def boundary_resources(samples, rss):
    first = min(s["slot_start_ns"] for s in samples)
    steady = min(s["slot_start_ns"] for s in samples if s["phase"] == "steady")
    last = max(s["slot_end_ns"] for s in samples)
    times = (1, first - 2, steady - 1, last + 1, last + 2, last + 3)
    return [
        dict(stage=stage, offset_ns=t, rss_bytes=rss, fd=10, threads=10, goroutines=10)
        for stage, t in zip(
            ("baseline", "ready", "warmup", "end", "validated", "after_close"), times
        )
    ]


class MemoryEligibility(unittest.TestCase):
    # The former denominator bias made these apparently passing ratios. Exercise
    # raw row derivation and the actual cell verdict, not a source-shape check.
    def bundle(self, root, evidence):
        (root / "rows").mkdir()
        measure.write(
            root / "manifest.json", {"mode": "smoke", "commands": [{"exit": 0}]}
        )
        selection = []
        for j, lane in enumerate(("go", "native")):
            selected = dict(
                lane=lane,
                seed=26011,
                workload="run64k",
                profile="controlled",
                concurrency=1,
                count=2,
                warmups_per_slot=0,
            )
            selection.append(selected)
            samples = []
            for ident, start in enumerate((1000000, 14000000, 27000000)):
                samples.append(
                    dict(
                        id=ident,
                        worker=0,
                        phase="first" if ident == 0 else "steady",
                        slot_start_ns=start - 500000,
                        start_ns=start,
                        owner_return_ns=start + 8000000,
                        delivered_ns=start + 9000000,
                        resource_ns=start + 10000000,
                        slot_end_ns=start + 11000000,
                        callback_count=1,
                        timeout=False,
                        complete=True,
                        result_class="ok",
                    )
                )
            rss = 350_000_000 if lane == "go" else 380_000_000
            resources = [
                dict(
                    stage="sample",
                    offset_ns=t * 1000000,
                    rss_bytes=rss,
                    fd=10,
                    threads=10,
                    goroutines=10,
                )
                for t in range(13, 39)
            ]
            resources += boundary_resources(samples, rss)
            row = dict(
                schema=2,
                row=selected,
                samples=samples,
                resources=resources,
                rss_interval_ns=1000000,
                throughput_scope=measure.THROUGHPUT_SCOPE,
                setup_heap_bytes=1000000,
                request_record_capacity=3,
                resource_record_capacity=120000,
                input_retention=dict(
                    prepared=3,
                    validated=3,
                    released=3,
                    remaining=0,
                    peak_inputs=1,
                    peak_payload_capacity_bytes=65536,
                    precreated_backend_runs=0,
                ),
            )
            if evidence == "legacy":
                row["schema"] = 1
                del row["input_retention"]
                del row["throughput_scope"]
            elif evidence == "eager":
                row["input_retention"].update(
                    peak_inputs=3, peak_payload_capacity_bytes=3 * 65536
                )
            elif evidence == "payload-only":
                row["input_retention"]["peak_payload_capacity_bytes"] = 3 * 65536
            elif evidence == "unreleased":
                row["input_retention"].update(released=0, remaining=3)
            elif evidence == "backend":
                row["row"]["profile"] = "real-valkey-on"
            measure.write(root / f"rows/row-{j:03d}.json", row)
        measure.write(root / "rows/selection.json", selection)

    def test_biased_memory_cannot_pass(self):
        for evidence in (
            "bounded",
            "legacy",
            "eager",
            "payload-only",
            "unreleased",
            "backend",
        ):
            with self.subTest(
                evidence=evidence
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.bundle(root, evidence)
                measure.derive(root, root / "derived.json")
                result = json.loads((root / "derived.json").read_text())
                (pair,) = result["pairs"]
                self.assertLess(pair["memory"], 1.20)
                self.assertTrue(
                    all(r["rss_status"] == "SAMPLED" for r in result["rows"])
                )
                self.assertTrue(
                    all(
                        r["p95_ns"] == 10000000 and r["responses_per_second"] > 0
                        for r in result["rows"]
                    )
                )
                cell = measure.summarize_cell(
                    ("run64k", "controlled", 1), [copy.copy(pair) for _ in range(3)]
                )
                self.assertEqual(
                    cell["memory"]["status"],
                    "PASS" if evidence == "bounded" else "INCONCLUSIVE",
                )
                self.assertEqual(
                    cell["status"], "PASS" if evidence == "bounded" else "INCONCLUSIVE"
                )
                if evidence == "bounded":
                    # The denominator includes all new setup/readback between calls.
                    self.assertAlmostEqual(
                        result["rows"][0]["responses_per_second"], 2e9 / 24500000
                    )

    def test_boundary_evidence_required_for_memory_only(self):
        stages = ("baseline", "ready", "warmup", "end", "validated", "after_close")
        cases = [("missing", stage, None) for stage in stages]
        cases += [
            ("rss", stage, value)
            for stage in (*stages, "sample")
            for value in (None, 0, -1, float("nan"), float("inf"), -float("inf"))
        ]
        cases += [
            ("time", stage, value)
            for stage in stages
            for value in (None, 0, -1, float("nan"))
        ]
        cases += [("order", stage, None) for stage in stages]
        cases += [("duplicate", stage, None) for stage in stages]
        cases += [
            ("samples-only", None, None),
            ("sparse", None, None),
            ("empty", None, None),
            ("no-usable-rss", None, None),
        ]
        cases += [("equal", stage, None) for stage in ("ready", "warmup", "end")]
        for kind, stage, value in cases:
            with self.subTest(
                kind=kind, stage=stage, value=value
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.bundle(root, "bounded")
                path = root / "rows/row-001.json"
                row = json.loads(path.read_text())
                if kind == "missing":
                    row["resources"] = [
                        r for r in row["resources"] if r["stage"] != stage
                    ]
                elif kind == "duplicate":
                    row["resources"].append(
                        copy.copy(
                            next(r for r in row["resources"] if r["stage"] == stage)
                        )
                    )
                elif kind == "empty":
                    row["resources"] = []
                elif kind == "samples-only":
                    row["resources"] = [
                        r for r in row["resources"] if r["stage"] == "sample"
                    ]
                elif kind == "sparse":
                    row["resources"] = [
                        r for r in row["resources"] if r["stage"] != "sample"
                    ]
                elif kind == "no-usable-rss":
                    for r in row["resources"]:
                        r["rss_bytes"] = 0
                else:
                    record = next(r for r in row["resources"] if r["stage"] == stage)
                    field = "rss_bytes" if kind == "rss" else "offset_ns"
                    if kind == "equal":
                        steady = [s for s in row["samples"] if s["phase"] == "steady"]
                        value = {
                            "ready": min(s["slot_start_ns"] for s in row["samples"]),
                            "warmup": min(s["slot_start_ns"] for s in steady),
                            "end": max(s["slot_end_ns"] for s in steady),
                        }[stage]
                    if kind == "order":
                        value = (
                            100000000 if stage in ("baseline", "ready", "warmup") else 2
                        )
                    if value is None:
                        del record[field]
                    else:
                        record[field] = value
                measure.write(path, row)
                measure.derive(root, root / "derived.json")
                result = json.loads((root / "derived.json").read_text())
                native = result["rows"][1]
                self.assertEqual(native["rss_status"], "INCONCLUSIVE")
                self.assertTrue(native["rss_inconclusive_reasons"])
                if stage:
                    self.assertTrue(
                        any(
                            stage in reason
                            for reason in native["rss_inconclusive_reasons"]
                        )
                    )
                if kind == "equal":
                    self.assertEqual(len(native["rss_inconclusive_reasons"]), 1)
                    self.assertIn(
                        {
                            "ready": "strictly precede",
                            "warmup": "strictly precede",
                            "end": "strictly follow",
                        }[stage],
                        native["rss_inconclusive_reasons"][0],
                    )
                self.assertEqual(native["p95_ns"], 10000000)
                self.assertAlmostEqual(native["responses_per_second"], 2e9 / 24500000)
                self.assertEqual(native["owner_service_rate_per_second"], 100)
                (pair,) = result["pairs"]
                self.assertFalse(pair["memory_conclusive"])
                cell = measure.summarize_cell(
                    ("run64k", "controlled", 1), [copy.copy(pair) for _ in range(3)]
                )
                self.assertEqual(cell["memory"]["status"], "INCONCLUSIVE")
                for metric in ("p95", "throughput", "owner_service_rate"):
                    self.assertEqual(cell[metric]["status"], "PASS")


class OwnerServiceRate(unittest.TestCase):
    def bundle(self, root):
        (root / "rows").mkdir()
        measure.write(
            root / "manifest.json", {"mode": "smoke", "commands": [{"exit": 0}]}
        )
        selection = []
        for seed in (26011, 26012, 26013):
            for lane in ("go", "native"):
                selected = dict(
                    lane=lane,
                    seed=seed,
                    workload="run64k",
                    profile="controlled",
                    concurrency=1,
                    count=100,
                    warmups_per_slot=0,
                )
                selection.append(selected)
                samples, clock = [], 100
                for ident in range(101):
                    # Exactly 100 steady C1 requests: 1 ms Go; 96 x 1 ms +
                    # 4 x 7 ms native. Both pay 2 ms of harness work per call.
                    duration = 7000000 if lane == "native" and ident > 96 else 1000000
                    start, end = clock + 1000000, clock + 1000000 + duration
                    samples.append(
                        dict(
                            id=ident,
                            worker=0,
                            phase="first" if ident == 0 else "steady",
                            slot_start_ns=clock,
                            start_ns=start,
                            owner_return_ns=end,
                            delivered_ns=end,
                            resource_ns=end,
                            slot_end_ns=end + 1000000,
                            callback_count=1,
                            timeout=False,
                            complete=True,
                            result_class="ok",
                        )
                    )
                    clock = end + 1000000 + (10 if ident == 0 else 0)
                resources = [
                    dict(
                        stage="sample",
                        offset_ns=t * 1000000,
                        rss_bytes=350000000,
                        fd=10,
                        threads=10,
                        goroutines=10,
                    )
                    for t in range(1, clock // 1000000 + 1)
                ]
                resources += boundary_resources(samples, 350000000)
                row = dict(
                    schema=2,
                    row=selected,
                    samples=samples,
                    resources=resources,
                    rss_interval_ns=1000000,
                    throughput_scope=measure.THROUGHPUT_SCOPE,
                    setup_heap_bytes=1000000,
                    request_record_capacity=101,
                    resource_record_capacity=120000,
                    input_retention=dict(
                        prepared=101,
                        validated=101,
                        released=101,
                        remaining=0,
                        peak_inputs=1,
                        peak_payload_capacity_bytes=65536,
                        precreated_backend_runs=0,
                    ),
                )
                measure.write(root / f"rows/row-{len(selection) - 1:03d}.json", row)
        measure.write(root / "rows/selection.json", selection)

    def test_tail_service_regression_cannot_hide_behind_harness_work(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.bundle(root)
            measure.derive(root, root / "derived.json")
            result = json.loads((root / "derived.json").read_text())
            for pair in result["pairs"]:
                self.assertEqual(pair["p95"], 1)
                self.assertAlmostEqual(pair["throughput"], 300 / 324)
                self.assertTrue(pair["memory_conclusive"])
            cell = measure.summarize_cell(("run64k", "controlled", 1), result["pairs"])
            for metric in ("p95", "throughput", "memory"):
                self.assertEqual(cell[metric]["status"], "PASS")
            self.assertEqual(cell["status"], "FAIL")
            for pair in result["pairs"]:
                self.assertAlmostEqual(pair["owner_service_rate"], 100 / 124)
            self.assertEqual(cell["owner_service_rate"]["passing_runs"], 0)

    def test_invalid_resource_durations_stop_derivation(self):
        for duration in (0, -1, None):
            with self.subTest(
                duration=duration
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.bundle(root)
                path = root / "rows/row-001.json"
                row = json.loads(path.read_text())
                sample = row["samples"][1]
                if duration is None:
                    del sample["resource_ns"]
                else:
                    sample["owner_return_ns"] = sample["delivered_ns"] = sample[
                        "resource_ns"
                    ] = (sample["start_ns"] + duration)
                measure.write(path, row)
                with self.assertRaisesRegex(ValueError, "resource duration"):
                    measure.derive(root, root / "derived.json")
                self.assertFalse((root / "derived.json").exists())


class DiagnosticCapture(unittest.TestCase):
    # Independent capture/authorization contract; no owner workload is executed.
    def test_reject_expanded_scope_and_comparative_derivation(self):
        for key, value in [
            ("lanes", "go,native"),
            ("profiles", "real-valkey-on"),
            ("workloads", "run64"),
            ("concurrency", "8"),
        ]:
            args = argparse.Namespace(**dict.fromkeys(measure.DIAGNOSTIC_SELECTORS))
            setattr(args, key, value)
            with self.assertRaisesRegex(ValueError, "diagnostic requires exact"):
                measure.diagnostic_selectors(args)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            measure.write(root / "manifest.json", dict(mode="diagnostic"))
            with self.assertRaisesRegex(ValueError, "no comparative derivation"):
                measure.derive(root, root / "forbidden.json")
            self.assertFalse((root / "forbidden.json").exists())

    def test_complete_capture_and_fail_closed_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "rows").mkdir()
            selected = dict(
                preflight=False,
                seed=26011,
                lane="native",
                profile="controlled",
                workload="run64k",
                concurrency=1,
                count=5000,
                warmups_per_slot=32,
                first_only=False,
                first_pair=0,
                order=0,
            )
            samples = [
                dict(
                    id=j,
                    worker=0,
                    phase="first" if j == 0 else ("warmup" if j <= 32 else "steady"),
                    slot_start_ns=1000000 + j * 2000000,
                    start_ns=1100000 + j * 2000000,
                    owner_return_ns=1200000 + j * 2000000,
                    delivered_ns=1300000 + j * 2000000,
                    resource_ns=1400000 + j * 2000000,
                    slot_end_ns=1500000 + j * 2000000,
                    callback_count=1,
                    complete=True,
                    timeout=False,
                    result_class="ok",
                )
                for j in range(5033)
            ]
            resources = boundary_resources(samples, 350000000)
            resources += [
                dict(
                    stage="sample",
                    offset_ns=1000000 + j * 1000000,
                    rss_bytes=350000000,
                    fd=10,
                    threads=10,
                    goroutines=10,
                )
                for j in range(10065)
            ]
            row = dict(
                schema=2,
                row=selected,
                capture_budget=measure.DIAGNOSTIC_BUDGET,
                samples=samples,
                resources=resources,
                rss_interval_ns=1000000,
                request_record_capacity=5033,
                resource_record_capacity=240000,
                throughput_scope=measure.THROUGHPUT_SCOPE,
                input_retention=dict(
                    prepared=5033,
                    validated=5033,
                    released=5033,
                    remaining=0,
                    peak_inputs=1,
                    peak_payload_capacity_bytes=65536,
                    precreated_backend_runs=0,
                ),
            )
            measure.write(
                root / "manifest.json",
                dict(mode="diagnostic", binary_sha256="synthetic"),
            )
            measure.write(root / "rows/selection.json", [selected])
            for corruption in (
                None,
                "timeout",
                "callback",
                "slot",
                "phase",
                "budget",
                "retained",
                "boundary",
                "sampler",
            ):
                with self.subTest(corruption=corruption):
                    current = copy.deepcopy(row)
                    if corruption == "timeout":
                        current["samples"][400]["timeout"] = True
                    if corruption == "callback":
                        current["samples"][400]["callback_count"] = 0
                    if corruption == "slot":
                        current["samples"][400]["slot_end_ns"] += 2000000
                    if corruption == "phase":
                        current["samples"][400]["phase"] = "warmup"
                    if corruption == "budget":
                        current["capture_budget"] = dict(
                            measure.DIAGNOSTIC_BUDGET, row_context_ns=120000000000
                        )
                    if corruption == "retained":
                        current["input_retention"]["remaining"] = 1
                    if corruption == "boundary":
                        current["resources"] = [
                            r for r in resources if r["stage"] != "warmup"
                        ]
                    if corruption == "sampler":
                        current["resource_record_capacity"] = 120000
                    measure.write(root / "rows/row-000.json", current)
                    if corruption is not None:
                        with self.assertRaisesRegex(ValueError, "STOP:"):
                            measure.diagnostic_result(root)
                    else:
                        measure.diagnostic_result(root)
                        result = json.loads((root / "diagnostic.json").read_text())
                        self.assertEqual(result["total_validated_closed_slots"], 5033)
                        self.assertEqual(
                            result["status"],
                            "DIAGNOSTIC_CAPTURE_COMPLETE_NO_PERFORMANCE_VERDICT",
                        )
                        self.assertEqual(result["rss_status"], "SAMPLED")
                        self.assertFalse(
                            any(
                                key in result
                                for key in (
                                    "p95_ns",
                                    "pairs",
                                    "cells",
                                    "responses_per_second",
                                )
                            )
                        )
                        (root / "diagnostic.json").unlink()
                    self.assertFalse((root / "diagnostic.json").exists())


if __name__ == "__main__":
    unittest.main()
