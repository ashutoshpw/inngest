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
from unittest import mock
import subprocess
import os
import time

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "measure", Path(__file__).with_name("response-owner-measure.py")
)
measure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure)


def command_evidence(path):
    measure.write(path.with_name(path.stem + "-command.json"), dict(
        status="COMPLETE", error="<nil>", argv=["synthetic-functional-or-row"],
        start_utc="2026-10-27T00:00:00Z", end_utc="2026-10-27T00:00:01Z",
    ))
    path.with_suffix(".log").write_text("synthetic receipt; no execution\n")


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


class FixedScreen(unittest.TestCase):
    # Synthetic scalar rows test the decision protocol, never owner performance.
    # Existing smoke/campaign/diagnostic tests cannot enforce screen completion,
    # independent rejection with sparse RSS, or a non-PASS overall outcome.
    def bundle(self, root, regression=None, failed_seeds=(26011, 26012, 26013), dense=False):
        (root / "rows").mkdir()
        measure.write(root / "manifest.json", dict(
            mode="screen", commands=[dict(exit=0)], screen_budget=measure.SCREEN_BUDGET,
            package_budget_seconds=900, package_elapsed_seconds=20,
            shutdown_grace_seconds=6, seal_reserve_seconds=4,
        ))
        command_evidence(root / "rows/preflight.json")
        (root / "rows/preflight.log").write_text("synthetic functional receipt; no execution\n")
        selection = []
        for seed in (26011, 26012, 26013):
            for workload in ("run64", "mixed8", "run64k"):
                for lane in ("go", "native"):
                    selected = dict(
                        preflight=False, seed=seed, lane=lane, workload=workload,
                        profile="controlled", concurrency=1, count=500,
                        warmups_per_slot=32, first_only=False, first_pair=0,
                        order=len(selection),
                    )
                    selection.append(selected)
                    failing = lane == "native" and seed in failed_seeds
                    raw = self.raw(selected, regression if failing else None, dense)
                    measure.write(root / f"rows/row-{len(selection)-1:03d}.json", raw)
                    command_evidence(root / f"rows/row-{len(selection)-1:03d}.json")
                    if seed == 26011:
                        port = dict(selected, preflight=True, count=0, warmups_per_slot=0,
                                    first_only=True, order=len(selection)-1)
                        measure.write(root / f"rows/port-preflight-{len(selection)-1:03d}.json", self.raw(port, None, False))
                        command_evidence(root / f"rows/port-preflight-{len(selection)-1:03d}.json")
        measure.write(root / "rows/selection.json", selection)

    def raw(self, selected, regression, dense):
        samples, clock = [], 1000000
        total = 1 + selected["warmups_per_slot"] + selected["count"]
        for ident in range(total):
            phase = "first" if ident == 0 else ("warmup" if ident <= selected["warmups_per_slot"] else "steady")
            duration = 1000000
            if regression == "p95" and ident % 10 == 0:
                duration = 1200000
            if regression == "service" and ident % 25 == 0:
                duration = 7000000
            preparation = 2000000 if regression == "throughput" else 1000000
            start, end = clock + preparation, clock + preparation + duration
            samples.append(dict(
                id=ident, worker=0, phase=phase, slot_start_ns=clock,
                start_ns=start, owner_return_ns=end, delivered_ns=end, resource_ns=end,
                slot_end_ns=end+1000000,
                callback_count=6 if selected["workload"] == "mixed8" else 1,
                timeout=False, complete=True, result_class="ok",
            ))
            clock = end + 1000010
        rss = 430000000 if regression == "memory" else 350000000
        if selected["count"]:
            resources = boundary_resources(samples, rss)
        else:
            resources = [dict(stage=stage, offset_ns=t, rss_bytes=rss, fd=10, threads=10, goroutines=10)
                         for stage, t in zip(("baseline", "ready", "warmup", "end", "validated", "after_close"),
                                             (1, 999998, clock+1, clock+2, clock+3, clock+4))]
        if dense:
            resources += [dict(stage="sample", offset_ns=t, rss_bytes=rss, fd=10, threads=10, goroutines=10)
                          for t in range(1000000, clock, 1000000)]
        return dict(schema=2, row=selected, capture_budget=measure.SCREEN_BUDGET,
                    samples=samples, resources=resources, rss_interval_ns=1000000,
                    setup_heap_bytes=1000000, throughput_scope=measure.THROUGHPUT_SCOPE,
                    request_record_capacity=total, resource_record_capacity=120000,
                    input_retention=dict(prepared=total, validated=total, released=total,
                                         remaining=0, peak_inputs=1,
                                         peak_payload_capacity_bytes=65536 if selected["workload"] == "run64k" else 64,
                                         precreated_backend_runs=0))

    def test_completed_rejections_survive_inconclusive_memory(self):
        for regression, metric in (("p95", "p95"), ("throughput", "throughput"), ("service", "owner_service_rate")):
            with self.subTest(regression=regression), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.bundle(root, regression)
                measure.derive(root, root / "derived.json")
                result = json.loads((root / "derived.json").read_text())
                self.assertEqual(result["status"], "SCREEN_NO_GO")
                self.assertEqual(len(result["pairs"]), 9)
                self.assertEqual(sum(r["count"] for r in result["rows"]), 9000)
                for cell in result["cells"]:
                    self.assertEqual(cell[metric]["status"], "FAIL")
                    self.assertEqual(cell["memory"]["status"], "INCONCLUSIVE")
                    self.assertEqual(cell["status"], "INCONCLUSIVE")
                    self.assertEqual(cell["screen_decision"], "NO_GO")
                    for other in {"p95", "throughput", "owner_service_rate"} - {metric}:
                        self.assertEqual(cell[other]["status"], "PASS")

    def test_seed_rule_and_no_overall_pass(self):
        for failed_seeds, dense, expected, regression in (
            ((26011,), False, "SCREEN_INCONCLUSIVE", "p95"),
            ((26011, 26012), False, "SCREEN_NO_GO", "p95"),
            ((), True, "SCREEN_PROMISING_NEEDS_BROADER_GATES", "p95"),
            ((26011, 26012, 26013), True, "SCREEN_NO_GO", "memory"),
        ):
            with self.subTest(seeds=failed_seeds, dense=dense), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.bundle(root, regression, failed_seeds, dense)
                measure.derive(root, root / "derived.json")
                result = json.loads((root / "derived.json").read_text())
                self.assertEqual(result["status"], expected)
                self.assertNotIn("PASS", result["status"])
                for cell in result["cells"]:
                    self.assertEqual(cell["p95"]["passing_runs"], 3 if regression == "memory" else 3-len(failed_seeds))
                    self.assertEqual(cell["memory"]["status"], "FAIL" if regression == "memory" else ("PASS" if dense else "INCONCLUSIVE"))

    def test_incomplete_corrupt_or_expired_package_has_no_verdict(self):
        for corruption in ("missing-seed", "duplicate-seed", "missing-row", "timeout", "functional-error", "port-error", "budget", "package-cap", "sampler-cap", "boundary", "phase", "unpaired", "provenance"):
            with self.subTest(corruption=corruption), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.bundle(root, "service")
                path = root / "rows/row-017.json"
                raw = json.loads(path.read_text())
                selection = json.loads((root / "rows/selection.json").read_text())
                if corruption == "missing-seed":
                    measure.write(root / "rows/selection.json", selection[:-6])
                elif corruption == "duplicate-seed":
                    selection[-1]["seed"] = 26012
                    measure.write(root / "rows/selection.json", selection)
                elif corruption == "missing-row":
                    path.unlink()
                elif corruption == "functional-error":
                    measure.write(root / "rows/preflight-command.json", dict(error="exit status 1"))
                elif corruption == "port-error":
                    port_path = root / "rows/port-preflight-005.json"
                    port = json.loads(port_path.read_text())
                    port["error"] = "mismatch"
                    measure.write(port_path, port)
                elif corruption in ("package-cap", "provenance"):
                    manifest = json.loads((root / "manifest.json").read_text())
                    if corruption == "package-cap":
                        manifest["package_elapsed_seconds"] = 900.001
                    else:
                        manifest["source_changed"] = ["source.go"]
                    measure.write(root / "manifest.json", manifest)
                elif corruption == "unpaired":
                    selection[1], selection[3] = selection[3], selection[1]
                    for j, row in enumerate(selection):
                        row["order"] = j
                    measure.write(root / "rows/selection.json", selection)
                else:
                    if corruption == "timeout":
                        raw["samples"][-1]["timeout"] = True
                    elif corruption == "budget":
                        raw["capture_budget"]["post_setup_watchdog_ns"] += 1
                    elif corruption == "sampler-cap":
                        raw["resources"] += [dict(stage="sample")] * 120001
                    elif corruption == "boundary":
                        raw["resources"] = [r for r in raw["resources"] if r["stage"] != "after_close"]
                    elif corruption == "phase":
                        raw["samples"][1]["phase"] = "steady"
                    measure.write(path, raw)
                with self.assertRaises((ValueError, OSError)):
                    measure.derive(root, root / "derived.json")
                self.assertFalse((root / "derived.json").exists())

    def test_successful_rows_require_each_child_command_and_log(self):
        for child in ("preflight", "port-preflight-005", "row-017"):
            for missing in ("command", "log", "final-update"):
                with self.subTest(child=child, missing=missing), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    self.bundle(root)
                    if missing == "command":
                        (root / f"rows/{child}-command.json").unlink()
                    elif missing == "log":
                        (root / f"rows/{child}.log").unlink()
                    else:
                        measure.write(root / f"rows/{child}-command.json", dict(status="STARTED", error="child incomplete"))
                    with self.assertRaisesRegex(ValueError, "child command/log evidence"):
                        measure.derive(root, root / "derived.json")
                    self.assertFalse((root / "derived.json").exists())

    def test_selectors_refuse_changed_scope(self):
        for key, value in (("lanes", "native"), ("profiles", "real-valkey-on"), ("workloads", "run64k"), ("concurrency", "1,8")):
            args = argparse.Namespace(**dict.fromkeys(measure.SCREEN_SELECTORS))
            setattr(args, key, value)
            with self.assertRaisesRegex(ValueError, "screen requires exact"):
                measure.screen_selectors(args)


class ScreenPackageControl(unittest.TestCase):
    # All child execution is mocked. A simulated clock tests the real capture
    # orchestrator's remaining-budget propagation/termination and failure seal.
    # These checks make no performance or workload-completion claim.
    def test_package_budget_includes_setup_and_kills_child(self):
        for failure in ("budget", "source", "bookkeeping"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                root, out = base / "checkout", base / "bundle"
                root.mkdir()
                (root / "source.go").write_text("unchanged")
                native = root / "rust/target/x86_64-unknown-linux-gnu/release/libresponse_continuation_bridge.a"
                native.parent.mkdir(parents=True)
                native.write_text("synthetic archive")
                binary = base / "executor.test"
                binary.write_text("synthetic binary")
                clock, processes = [100.0], []

                def git(argv, **kwargs):
                    if "--show-toplevel" in argv:
                        clock[0] += 10  # setup consumes the same budget
                        return str(root) + "\n"
                    if "ls-files" in argv:
                        return b"source.go\0"
                    if "diff" in argv:
                        return b""
                    return "synthetic\n"

                class Child:
                    pid, returncode = 424242, None
                    def __init__(self, argv, **kwargs):
                        self.argv = argv
                        read_fd, write_fd = os.pipe()
                        os.close(write_fd)
                        self.stdout = os.fdopen(read_fd, "rb")
                        processes.append(self)
                    def __enter__(self):
                        return self
                    def __exit__(self, *args):
                        return False
                    def poll(self):
                        return self.returncode
                    def wait(self, timeout=None):
                        if self.returncode is not None:
                            return self.returncode
                        if timeout is None:
                            self.returncode = -9
                            return -9
                        # Preserve the in-progress row exactly as a real child
                        # would before a package kill, without running any owner.
                        (out / "rows").mkdir(exist_ok=True)
                        measure.write(out / "rows/row-000.json", dict(status="STARTED", error="child incomplete"))
                        if failure == "source":
                            (root / "source.go").write_text("changed during capture")
                        elif failure == "budget":
                            clock[0] = 991  # 891 seconds since capture entry; inside hard budget
                        else:
                            self.returncode = 0
                            return 0
                        raise subprocess.TimeoutExpired(self.argv, timeout)

                original_ledger = measure.artifact_ledger
                def ledger(path):
                    original_ledger(path)
                    if failure == "bookkeeping":
                        clock[0] = 991  # expiry after child success still stops package

                def kill_group(pid, sig):
                    if sig == measure.signal.SIGKILL:
                        processes[0].returncode = -9
                    if sig == 0:
                        raise ProcessLookupError()

                args = argparse.Namespace(mode="screen", binary=str(binary), out=str(out),
                                          go="synthetic-go", declare_only=failure == "bookkeeping",
                                          **dict.fromkeys(measure.SCREEN_SELECTORS))
                with mock.patch.object(measure.subprocess, "check_output", side_effect=git), \
                     mock.patch.object(measure.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "version", "")), \
                     mock.patch.object(measure.subprocess, "Popen", Child), \
                     mock.patch.object(measure.time, "monotonic", side_effect=lambda: clock[0]), \
                     mock.patch.object(measure, "artifact_ledger", side_effect=ledger), \
                     mock.patch.object(measure.os, "killpg", side_effect=kill_group) as kill:
                    with self.assertRaisesRegex(ValueError, "provenance changed" if failure == "source" else "wall budget expired"):
                        measure.capture(args)
                self.assertEqual(len(processes), 1)
                argv = processes[0].argv
                remaining = next(a.split("=", 1)[1] for a in argv if a.startswith("-response-measure-package-remaining="))
                self.assertEqual(remaining, "880.000000000s")
                if failure == "bookkeeping":
                    kill.assert_not_called()
                else:
                    self.assertEqual([c for c in kill.call_args_list if c.args[1] != 0], [
                        mock.call(424242, measure.signal.SIGUSR1),
                        mock.call(424242, measure.signal.SIGKILL),
                    ])
                manifest = json.loads((out / "manifest.json").read_text())
                self.assertIn("STOP:", manifest["stop_reason"])
                self.assertEqual(json.loads((out / "rows/row-000.json").read_text())["status"], "STARTED")
                self.assertTrue((out / "artifacts.json").is_file())
                self.assertFalse((out / "derived.json").exists())


class StopEvidence(unittest.TestCase):
    # Real process/session, real SIGALRM/SIGUSR1 and actual controller sealing.
    # The helper substitutes only for the expensive owner workload, keeping
    # completed records in memory until the diagnostic stop request arrives.
    def test_real_stop_preserves_completed_records_and_output(self):
        for reason in ("expiry", "provenance"):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                root, out = base / "checkout", base / "bundle"
                root.mkdir()
                source = root / "source.go"
                source.write_text("unchanged")
                (root / "pkg/execution/executor").mkdir(parents=True)
                native = root / "rust/target/x86_64-unknown-linux-gnu/release/libresponse_continuation_bridge.a"
                native.parent.mkdir(parents=True)
                native.write_text("archive")
                binary = base / "capture-helper"
                binary.write_text("""#!/usr/bin/env python3
import json, os, signal, sys, time
from pathlib import Path
out = Path(next(a.split('=', 1)[1] for a in sys.argv if a.startswith('-response-measure-out=')))
out.mkdir()
path = out / 'row-000.json'
path.write_text(json.dumps({'status': 'STARTED', 'error': 'child incomplete'}))
records = [{'id': j, 'complete': True} for j in range(4)]
def stop(*_):
    path.write_text(json.dumps({'samples': records, 'stop_reason': 'cooperative_stop', 'error': 'STOP'}))
    print('diagnostic flush complete', flush=True)
    sys.exit(2)
signal.signal(signal.SIGUSR1, stop)
print('completed four records', flush=True)
if os.environ.get('CONTROL_CHANGE'):
    Path(os.environ['CONTROL_CHANGE']).write_text('changed')
while True:
    time.sleep(.01)
""")
                binary.chmod(0o700)
                def git(argv, **kwargs):
                    if "--show-toplevel" in argv:
                        return str(root) + "\n"
                    if "ls-files" in argv:
                        return b"source.go\0"
                    if "diff" in argv:
                        return b""
                    return "synthetic\n"
                args = argparse.Namespace(mode="screen", binary=str(binary), out=str(out),
                                          go="unused", declare_only=False,
                                          **dict.fromkeys(measure.SCREEN_SELECTORS))
                started = time.monotonic()
                with mock.patch.object(measure, "SCREEN_SECONDS", 3), \
                     mock.patch.object(measure, "SCREEN_SHUTDOWN_GRACE", 1), \
                     mock.patch.object(measure, "SCREEN_SEAL_RESERVE", .5), \
                     mock.patch.object(measure.subprocess, "check_output", side_effect=git), \
                     mock.patch.object(measure.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "version", "")), \
                     mock.patch.dict(os.environ, {"CONTROL_CHANGE": str(source) if reason == "provenance" else ""}):
                    with self.assertRaisesRegex(ValueError, "provenance changed" if reason == "provenance" else "wall budget expired"):
                        measure.capture(args)
                row = json.loads((out / "rows/row-000.json").read_text())
                self.assertEqual([r["id"] for r in row.get("samples", []) if r["complete"]], [0, 1, 2, 3])
                self.assertLess(time.monotonic() - started, 3)
                self.assertIn("completed four records", (out / "run.log").read_text())
                self.assertIn("diagnostic flush complete", (out / "run.log").read_text())
                manifest = json.loads((out / "manifest.json").read_text())
                self.assertIn("STOP:", manifest["stop_reason"])
                self.assertEqual(manifest["commands"][-1]["exit"], 2)
                self.assertEqual(manifest["commands"][-1]["status"], "INCOMPLETE")
                self.assertTrue((out / "artifacts.json").is_file())
                with self.assertRaises(ValueError):
                    measure.derive(out, out / "derived.json")
                self.assertFalse((out / "derived.json").exists())

    # Authoring gate: real Python group STOP must traverse the real Go parent,
    # command runner and row. Separate synthetic/row tests cannot detect lost
    # signals or admission during setup across that process tree. All barriers
    # and the five-slot configuration live in the tagged measurement test scope.
    def test_real_python_go_parent_row_stop(self):
        binary = os.environ.get("RESPONSE_MEASURE_CONTROL_BINARY")
        if not binary:
            self.skipTest("set RESPONSE_MEASURE_CONTROL_BINARY to the tagged Go test binary")
        binary = str(Path(binary).resolve())
        root = Path(__file__).resolve().parents[1]
        for stage in ("setup", "preparation", "prepared", "published"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                out = base / "bundle"
                wrapper = base / "measurement-control"
                wrapper.write_text("#!/usr/bin/env python3\nimport os, sys\n" +
                                   "binary = " + repr(binary) + "\n" +
                                   "os.execv(binary, [binary, '-test.run=^TestResponseMeasureCombinedControl$'] + [a for a in sys.argv[1:] if not a.startswith('-test.run=')])\n")
                wrapper.chmod(0o700)
                args = argparse.Namespace(mode="screen", binary=str(wrapper), out=str(out),
                                          go=os.environ.get("RESPONSE_MEASURE_CONTROL_GO", "go"), declare_only=False,
                                          **dict.fromkeys(measure.SCREEN_SELECTORS))
                original_remaining = measure.ScreenPackage.remaining
                observed = []
                def remaining(package):
                    marker = out / "rows/row-000.json.ready"
                    if marker.exists() and not observed:
                        observed.append(json.loads(marker.read_text()))
                        raise ValueError("STOP: combined control barrier " + stage)
                    return original_remaining(package)
                original_check_output = subprocess.check_output
                def metadata(argv, **kwargs):
                    if argv[:3] == ["git", "ls-files", "-z"]:
                        # The control observes one real, immutable source path;
                        # it does not archive the full repository as a screen.
                        return b"pkg/execution/executor/response_continuation_measure_test.go\0"
                    return original_check_output(argv, **kwargs)
                started = time.monotonic()
                try:
                    with mock.patch.object(measure.ScreenPackage, "remaining", remaining), \
                         mock.patch.object(measure, "SCREEN_SECONDS", 30), \
                         mock.patch.object(measure.subprocess, "check_output", side_effect=metadata), \
                         mock.patch.dict(os.environ, {"RESPONSE_MEASURE_COMBINED_CONTROL": "1", "RESPONSE_MEASURE_CONTROL_BARRIER": stage}):
                        with self.assertRaisesRegex(ValueError, "combined control barrier"):
                            measure.capture(args)
                    self.assertEqual(len(observed), 1)
                    self.assertLess(time.monotonic() - started, 30)
                    row = json.loads((out / "rows/row-000.json").read_text())
                    row_log = (out / "rows/row-000.log").read_text()
                    if stage == "setup":
                        self.assertNotIn("control input preparation", row_log)
                    self.assertEqual(row["stop_reason"], "cooperative_stop")
                    self.assertEqual(len(row["samples"]), 5)
                    admitted = [s for s in row["samples"] if s["slot_start_ns"] > 0]
                    self.assertEqual(len(admitted), 2 if stage == "published" else (0 if stage == "setup" else 1))
                    if stage == "setup":
                        self.assertEqual(row["input_retention"]["prepared"], 0)
                        self.assertTrue(all(s["result_class"] == "not_started" for s in row["samples"]))
                    elif stage == "published":
                        self.assertEqual([s["id"] for s in row["samples"] if s["complete"] and s["slot_end_ns"] > 0], [0, 1])
                        self.assertEqual(row["input_retention"]["released"], 2)
                    else:
                        self.assertFalse(any(s["complete"] for s in row["samples"]))
                        self.assertEqual(row["input_retention"]["remaining"], int(stage == "prepared"))
                        self.assertIn(stage, row["samples"][0]["result_class"])
                    self.assertIn("control barrier: " + stage, row_log)
                    self.assertIn("control parent starting", (out / "run.log").read_text())
                    for name, status in (("preflight", "COMPLETE"), ("port-preflight-000", "COMPLETE"), ("row-000", "INCOMPLETE")):
                        command = json.loads((out / ("rows/" + name + "-command.json")).read_text())
                        self.assertEqual(command["status"], status)
                        self.assertIn("end_utc", command)
                        self.assertTrue((out / ("rows/" + name + ".log")).is_file())
                    manifest = json.loads((out / "manifest.json").read_text())
                    self.assertIn("combined control barrier", manifest["stop_reason"])
                    self.assertEqual(manifest["commands"][-1]["status"], "INCOMPLETE")
                    self.assertTrue((out / "artifacts.json").is_file())
                    self.assertTrue((out / "CAPTURE_INCOMPLETE").is_file())
                    with self.assertRaisesRegex(ValueError, "incomplete capture/failure seal"):
                        measure.derive(out, out / "derived.json")
                    self.assertFalse((out / "derived.json").exists())
                    # The Go parent reaps its row, and Python reaps the group
                    # leader. A completed wait alone would not prove this.
                    for pid in (observed[0]["pid"], observed[0]["parent_pid"]):
                        with self.assertRaises(ProcessLookupError):
                            os.kill(pid, 0)
                    with self.assertRaises(ProcessLookupError):
                        os.killpg(observed[0]["parent_pid"], 0)
                finally:
                    evidence = os.environ.get("RESPONSE_MEASURE_CONTROL_EVIDENCE")
                    if evidence and out.exists():
                        import shutil
                        shutil.copytree(out, Path(evidence) / stage)

    def test_filesystem_write_failures_remain_fatal_even_when_sealing_fails(self):
        for failure in ("initial-manifest", "final-manifest", "log-open", "log-write", "artifact-ledger"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                root, out = base / "checkout", base / "bundle"
                root.mkdir()
                (root / "source.go").write_text("source")
                (root / "pkg/execution/executor").mkdir(parents=True)
                native = root / "rust/target/x86_64-unknown-linux-gnu/release/libresponse_continuation_bridge.a"
                native.parent.mkdir(parents=True)
                native.write_text("archive")
                binary = base / "capture-helper"
                binary.write_text("""#!/usr/bin/env python3
import os
from pathlib import Path
print('child output before write failure', flush=True)
trap = os.environ.get('CONTROL_WRITE_TRAP')
if trap:
    Path(trap).mkdir()
""")
                binary.chmod(0o700)
                def git(argv, **kwargs):
                    if "--show-toplevel" in argv:
                        return str(root) + "\n"
                    if "ls-files" in argv:
                        if failure == "initial-manifest":
                            (out / "manifest.json.tmp").mkdir(exist_ok=True)
                        return b"source.go\0"
                    if "diff" in argv:
                        return b""
                    return "synthetic\n"
                def version(*args, **kwargs):
                    if failure == "log-open":
                        (out / "run.log").mkdir(exist_ok=True)
                    elif failure == "log-write" and not (out / "run.log").exists():
                        (out / "run.log").symlink_to("/dev/full")
                    return subprocess.CompletedProcess([], 0, "version", "")
                trap = {"final-manifest": "manifest.json.tmp", "artifact-ledger": "artifacts.json.tmp"}.get(failure)
                expected = "No space left" if failure == "log-write" else ("run.log" if failure == "log-open" else (trap or "manifest.json.tmp"))
                args = argparse.Namespace(mode="screen", binary=str(binary), out=str(out),
                                          go="unused", declare_only=False,
                                          **dict.fromkeys(measure.SCREEN_SELECTORS))
                with mock.patch.object(measure.subprocess, "check_output", side_effect=git), \
                     mock.patch.object(measure.subprocess, "run", side_effect=version), \
                     mock.patch.dict(os.environ, {"CONTROL_WRITE_TRAP": str(out / trap) if trap else ""}):
                    with self.assertRaisesRegex(OSError, expected):
                        measure.capture(args)
                self.assertTrue((out / "CAPTURE_INCOMPLETE").is_file())
                with self.assertRaisesRegex(ValueError, "incomplete capture/failure seal"):
                    measure.derive(out, out / "derived.json")
                self.assertFalse((out / "derived.json").exists())


if __name__ == "__main__":
    unittest.main()
