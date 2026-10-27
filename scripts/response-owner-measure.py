#!/usr/bin/env python3
"""Capture/derive matched response-owner experiments. Smoke is not performance evidence.

Build separately with the protocol's Go/Cgo command. Run this from the repository
root. A new bundle preserves source, binaries' hashes, commands and contaminated
rows. Never rerun in an existing bundle or drop a failed row.
"""

import argparse
import hashlib
import json
import math
import os
import signal
from pathlib import Path
import statistics
import subprocess
import sys
import tarfile
from datetime import datetime, timezone


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n")


DIAGNOSTIC_BUDGET = dict(
    post_setup_watchdog_ns=210_000_000_000,
    row_context_ns=240_000_000_000,
    test_timeout_ns=260_000_000_000,
    outer_kill_ns=270_000_000_000,
    request_deadline_ns=20_000_000_000,
    resource_records=240000,
)
DIAGNOSTIC_SELECTORS = dict(
    lanes="native", profiles="controlled", workloads="run64k", concurrency="1"
)


def diagnostic_selectors(args):
    for key, value in DIAGNOSTIC_SELECTORS.items():
        selected = getattr(args, key)
        if selected is not None and selected != value:
            raise ValueError(
                "diagnostic requires exact native/controlled/run64k/C1 selectors"
            )
        setattr(args, key, value)


def readable_host():
    result = {}
    for name in (
        "/proc/loadavg",
        "/proc/meminfo",
        "/proc/self/status",
        "/proc/self/cgroup",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/cpu.stat",
    ):
        try:
            result[name] = Path(name).read_text()
        except OSError as exc:
            result[name] = str(exc)
    # CPU limits can live on the process cgroup or an ancestor, not cgroup root.
    try:
        line = next(
            s
            for s in Path("/proc/self/cgroup").read_text().splitlines()
            if s.startswith("0::")
        )
        base = Path("/sys/fs/cgroup")
        current = base / line[3:].lstrip("/")
        for _ in range(32):
            for field in ("cpu.max", "cpu.stat", "cpuset.cpus.effective"):
                name = current / field
                try:
                    result[str(name)] = name.read_text()
                except OSError as exc:
                    result[str(name)] = str(exc)
            if current == base:
                break
            current = current.parent
    except (OSError, StopIteration) as exc:
        result["process_cgroup_limits"] = str(exc)
    return result


def capture(args):
    if args.mode == "diagnostic":
        diagnostic_selectors(args)
    root = Path(
        subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
    )
    out = Path(args.out).resolve()
    if root == out or root in out.parents:
        raise ValueError("artifact bundle must be outside checkout")
    out.mkdir(mode=0o700)  # exclusive; preserve every earlier failure
    binary = Path(args.binary).resolve()
    native = (
        root
        / "rust/target/x86_64-unknown-linux-gnu/release/libresponse_continuation_bridge.a"
    )
    names = sorted(
        set(
            subprocess.check_output(
                ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                cwd=root,
            ).split(b"\0")
        )
        - {b""}
    )
    sources = {
        os.fsdecode(n): sha(root / os.fsdecode(n))
        for n in names
        if (root / os.fsdecode(n)).is_file()
    }
    with tarfile.open(out / "source.tar.gz", "w:gz") as archive:
        for name in sources:
            archive.add(root / name, arcname=name, recursive=False)
    write(out / "source-manifest.json", sources)
    (out / "source.patch").write_bytes(
        subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=root)
    )
    manifest = {
        "schema": 1,
        "mode": args.mode,
        "start_utc": datetime.now(timezone.utc).isoformat(),
        "base_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip(),
        "branch": subprocess.check_output(
            ["git", "branch", "--show-current"], cwd=root, text=True
        ).strip(),
        "source_archive_sha256": sha(out / "source.tar.gz"),
        "binary": str(binary),
        "binary_sha256": sha(binary),
        "native_static_sha256": sha(native),
        "source_manifest_sha256": sha(out / "source-manifest.json"),
        "policy": "UNCALIBRATED: resource p95<=1.10, closed-loop throughput including input preparation/validation>=0.90, separate owner service rate (inverse mean resource duration at fixed C)>=0.90, whole host peak RSS<=1.20; median pass and >=2/3 runs for every metric in every cell; sparse RSS INCONCLUSIVE; no process-transport selection",
        "commands": [],
        "capture_argv": sys.argv,
        "reducer_sha256": sha(Path(__file__)),
        "diagnostic_budget": DIAGNOSTIC_BUDGET if args.mode == "diagnostic" else None,
        "preflight_scope": (
            "none in diagnostic capture; standalone focused functional/capture-control checks required; no Valkey or extra measurement rows"
            if args.mode == "diagnostic"
            else "unchanged functional and lane/profile/workload port preflights"
        ),
        "host_before": readable_host(),
    }
    if args.mode == "diagnostic":
        manifest["policy"] = (
            "DIAGNOSTIC ONLY: capture completeness and budget evidence; no comparative rates/percentiles/ratios, gate verdict or campaign PASS"
        )
    write(out / "manifest.json", manifest)
    env = dict(os.environ, GOMAXPROCS="8")
    env.pop("RESPONSE_MEASURE_CHILD", None)
    env.pop("RESPONSE_MEASURE_ROW_PATH", None)
    for argv in [
        [args.go, "version"],
        ["rustc", "+1.94.0", "--version"],
        ["cc", "--version"],
    ]:
        r = subprocess.run(argv, capture_output=True, text=True, cwd=root)
        manifest["commands"].append(
            {"argv": argv, "exit": r.returncode, "output": r.stdout + r.stderr}
        )
        if r.returncode:
            write(out / "manifest.json", manifest)
            raise ValueError(f"version command failed: {argv}")
    argv = [
        str(binary),
        "-test.run=^TestResponseContinuationMeasure$",
        "-test.v",
        "-test.timeout=8h",
        f"-response-measure-mode={args.mode}",
        f'-response-measure-out={out / "rows"}',
    ]
    if args.mode == "diagnostic":
        argv[3] = "-test.timeout=260s"
    for key in ["lanes", "profiles", "workloads", "concurrency"]:
        if getattr(args, key):
            argv.append(f"-response-measure-{key}={getattr(args, key)}")
    command = {
        "argv": argv,
        "cwd": str(root / "pkg/execution/executor"),
        "env_overrides": {
            "GOMAXPROCS": "8",
            "RESPONSE_MEASURE_VALKEY_ADDR": env.get("RESPONSE_MEASURE_VALKEY_ADDR"),
        },
        "environment": {
            k: env.get(k)
            for k in (
                "GOMAXPROCS",
                "GOTOOLCHAIN",
                "GOTMPDIR",
                "TMPDIR",
                "GOFLAGS",
                "CGO_ENABLED",
                "CGO_CFLAGS",
                "CGO_LDFLAGS",
                "GOMEMLIMIT",
                "GOGC",
                "GODEBUG",
                "RUSTUP_TOOLCHAIN",
            )
        },
        "start_utc": datetime.now(timezone.utc).isoformat(),
    }
    manifest["commands"].append(command)
    write(out / "manifest.json", manifest)
    with open(out / "run.log", "wb") as log:
        if args.mode == "diagnostic":
            with subprocess.Popen(
                argv,
                cwd=command["cwd"],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            ) as process:
                try:
                    process.wait(timeout=270)
                except subprocess.TimeoutExpired:
                    command["stop_reason"] = (
                        "outer_kill_expired; request timeout/native hang not established"
                    )
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                r = process
        else:
            r = subprocess.run(
                argv, cwd=command["cwd"], env=env, stdout=log, stderr=subprocess.STDOUT
            )
    command.update(exit=r.returncode, end_utc=datetime.now(timezone.utc).isoformat())
    manifest["host_after"] = readable_host()
    changed = [
        name
        for name, digest in sources.items()
        if not (root / name).is_file() or sha(root / name) != digest
    ]
    manifest.update(
        source_changed=changed,
        binary_changed=sha(binary) != manifest["binary_sha256"],
        native_changed=sha(native) != manifest["native_static_sha256"],
    )
    write(out / "manifest.json", manifest)
    # Hash raw logs/commands/rows even on failure; no favorable-only filtering.
    write(
        out / "artifacts.json",
        {
            str(p.relative_to(out)): sha(p)
            for p in sorted(out.rglob("*"))
            if p.is_file() and p.name != "artifacts.json"
        },
    )
    if (
        r.returncode
        or changed
        or manifest["binary_changed"]
        or manifest["native_changed"]
    ):
        raise ValueError(f"STOP: execution/source failure; preserve {out}")
    if args.mode == "diagnostic":
        diagnostic_result(out)
    elif args.mode != "plan":
        derive(out, out / "derived.json")
    write(
        out / "artifacts.json",
        {
            str(p.relative_to(out)): sha(p)
            for p in sorted(out.rglob("*"))
            if p.is_file() and p.name != "artifacts.json"
        },
    )
    print(out)


THROUGHPUT_SCOPE = "closed-loop input preparation, owner/resource completion, validation and input release; backend runs precreated"


def input_retention_status(row, total, resource_capacity=120000):
    # Missing/old evidence cannot turn a setup-bloated RSS ratio into a pass.
    r = row.get("input_retention", {})
    c = row["row"]["concurrency"]
    payload_size = 65527 if row["row"]["workload"] == "run64k" else 55
    payload_budget = c * (65536 if row["row"]["workload"] == "run64k" else 64)
    precreated = 0 if row["row"]["profile"] == "controlled" else total
    valid = (
        row.get("schema") == 2
        and row.get("throughput_scope") == THROUGHPUT_SCOPE
        and all(r.get(k) == total for k in ("prepared", "validated", "released"))
        and r.get("remaining") == 0
        and 1 <= r.get("peak_inputs", 0) <= c
        and r.get("peak_inputs", 0) * payload_size
        <= r.get("peak_payload_capacity_bytes", 0)
        <= payload_budget
        and r.get("precreated_backend_runs") == precreated
        and row.get("request_record_capacity") == total
        and row.get("resource_record_capacity") == resource_capacity
    )
    return "BOUNDED_INPUTS" if valid else "INCONCLUSIVE_INPUT_RETENTION"


def rss_evidence(row, samples, first, last):
    # Capture appends sampler records after warmup, so validate lifecycle times,
    # not array order. Post-validation/close snapshots prove completion but do
    # not expand the existing baseline-through-end peak window.
    stages = ("baseline", "ready", "warmup", "end", "validated", "after_close")
    resources = [
        r for r in row.get("resources", []) if r.get("stage") in (*stages, "sample")
    ]
    reasons, boundaries = [], {}
    for stage in stages:
        records = [r for r in resources if r["stage"] == stage]
        if len(records) != 1:
            reasons.append(
                f"{stage}: expected one boundary snapshot, found {len(records)}"
            )
        else:
            boundaries[stage] = records[0]
    for r in resources:
        rss, offset = r.get("rss_bytes"), r.get("offset_ns")
        if not usable_rss(rss):
            reasons.append(
                f"{r['stage']}: missing/invalid RSS (requires finite positive bytes)"
            )
        if type(offset) is not int or offset <= 0:
            reasons.append(f"{r['stage']}: missing/invalid monotonic offset_ns")
    if all(
        stage in boundaries
        and type(boundaries[stage].get("offset_ns")) is int
        and boundaries[stage]["offset_ns"] > 0
        for stage in stages
    ):
        times = [boundaries[stage]["offset_ns"] for stage in stages]
        for a, b, ta, tb in zip(stages, stages[1:], times, times[1:]):
            if ta >= tb:
                reasons.append(f"{a}/{b}: boundary snapshots out of lifecycle order")
        start_key = "slot_start_ns" if row.get("schema") == 2 else "start_ns"
        end_key = "slot_end_ns" if row.get("schema") == 2 else "resource_ns"
        before = [s for s in samples if s["phase"] != "steady"]
        if times[1] >= min(s[start_key] for s in samples):
            reasons.append("ready: snapshot must strictly precede first request start")
        if (before and times[2] < max(s[end_key] for s in before)) or times[2] >= first:
            reasons.append(
                "warmup: snapshot must follow first/warmup completion and strictly precede steady start"
            )
        if times[3] <= last:
            reasons.append("end: snapshot must strictly follow steady completion")
    interval = row.get("rss_interval_ns")
    points = sorted(
        r["offset_ns"]
        for r in resources
        if r["stage"] == "sample"
        and type(r.get("offset_ns")) is int
        and first <= r["offset_ns"] <= last
    )
    edges = [first] + points + [last]
    dense = (
        type(interval) is int
        and interval > 0
        and len(points) >= 10
        and last - first >= 10 * interval
        and max(b - a for a, b in zip(edges, edges[1:])) <= 5 * interval
    )
    if not dense:
        reasons.append(
            "sample: insufficient steady RSS sampling density/duration/coverage"
        )
    peak = max(
        (
            r["rss_bytes"]
            for r in resources
            if r["stage"] in ("baseline", "ready", "warmup", "sample", "end")
            and usable_rss(r.get("rss_bytes"))
        ),
        default=None,
    )
    return resources, peak, reasons


def usable_rss(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def summarize_cell(key, pairs):
    cell = {"workload": key[0], "profile": key[1], "concurrency": key[2]}
    passes = []
    for metric, limit, upper in [
        ("p95", 1.10, True),
        ("throughput", 0.90, False),
        ("owner_service_rate", 0.90, False),
        ("memory", 1.20, True),
    ]:
        values = [p[metric] for p in pairs if p[metric] is not None]
        median = statistics.median(values) if values else None
        count = sum(v <= limit if upper else v >= limit for v in values)
        good = (
            median is not None
            and (median <= limit if upper else median >= limit)
            and count >= 2
        )
        status = "PASS" if good else "FAIL"
        if metric == "memory" and not all(p["memory_conclusive"] for p in pairs):
            good, status = False, "INCONCLUSIVE"
        cell[metric] = {"median_ratio": median, "passing_runs": count, "status": status}
        passes.append(good)
    cell["status"] = "PASS" if all(passes) else "FAIL"
    if cell["memory"]["status"] == "INCONCLUSIVE":
        cell["status"] = "INCONCLUSIVE"
    return cell


def validate_samples(row, selected, p):
    expected_results = {
        "retryable": "retryable_step_error",
        "enqueue-error": "enqueue_commit_error",
    }
    samples = row["samples"]
    bounded = row.get("schema") == 2
    n = 1 + selected["warmups_per_slot"] * selected["concurrency"] + selected["count"]
    if len(samples) != n or sorted(s["id"] for s in samples) != list(range(n)):
        raise ValueError(f"STOP: missing/duplicated requests {p}")
    for sample in samples:
        start, resource = sample.get("start_ns"), sample.get("resource_ns")
        if (
            not isinstance(start, int)
            or not isinstance(resource, int)
            or resource <= start
        ):
            raise ValueError(f"STOP: missing/nonpositive resource duration {p}")
        want_callbacks = {"mixed8": 6, "enqueue-error": 0}.get(selected["workload"], 1)
        if sample["callback_count"] != want_callbacks:
            raise ValueError(f"STOP: incorrect callbacks {p}")
        if (
            sample["timeout"]
            or not sample["complete"]
            or sample["result_class"]
            != expected_results.get(selected["workload"], "ok")
        ):
            raise ValueError(f"STOP: timeout/incomplete/wrong result {p}: {sample}")
        if (
            not sample["start_ns"]
            <= sample["owner_return_ns"]
            <= sample["delivered_ns"]
            <= sample["resource_ns"]
        ):
            raise ValueError(f"STOP: inconsistent boundaries {p}")
        if (
            bounded
            and not 0
            < sample["slot_start_ns"]
            <= sample["start_ns"]
            <= sample["resource_ns"]
            <= sample["slot_end_ns"]
        ):
            raise ValueError(f"STOP: inconsistent setup/validation boundaries {p}")
    for worker in range(selected["concurrency"]):
        owned = sorted(
            (s for s in samples if s["worker"] == worker), key=lambda s: s["start_ns"]
        )
        if any(
            a["slot_end_ns" if bounded else "resource_ns"]
            > b["slot_start_ns" if bounded else "start_ns"]
            for a, b in zip(owned, owned[1:])
        ):
            raise ValueError(f"STOP: slot reuse before cleanup {p}")
    return samples, bounded, n


def derive(out, destination):
    destination = destination.resolve()
    if destination.exists():
        raise ValueError("STOP: derived destination already exists; choose a new path")
    manifest = json.loads((out / "manifest.json").read_text())
    if manifest["mode"] == "diagnostic":
        raise ValueError(
            "STOP: diagnostic capture has no comparative derivation or campaign verdict"
        )
    if (
        any(c.get("exit", 1) != 0 for c in manifest["commands"])
        or manifest.get("source_changed")
        or manifest.get("binary_changed")
        or manifest.get("native_changed")
    ):
        raise ValueError("STOP: failed command or changed source/binary")
    selection = json.loads((out / "rows/selection.json").read_text())
    rows = []
    for j, selected in enumerate(selection):
        p = out / f"rows/row-{j:03d}.json"
        row = json.loads(p.read_text())
        if row.get("error") or row["row"] != selected:
            raise ValueError(f"STOP: wrong/missing/contaminated row {p}")
        samples, bounded, n = validate_samples(row, selected, p)
        durations = sorted(
            s["resource_ns"] - s["start_ns"] for s in samples if s["phase"] == "steady"
        )
        if len(durations) != selected["count"]:
            raise ValueError(f"STOP: wrong steady count {p}")
        if not durations:
            continue  # first observations stay raw; no pooled percentile
        steady = [s for s in samples if s["phase"] == "steady"]
        first = min(s["slot_start_ns" if bounded else "start_ns"] for s in steady)
        last = max(s["slot_end_ns" if bounded else "resource_ns"] for s in steady)
        elapsed = last - first
        occupancy = sum(durations)
        costs = None
        if bounded:
            # Sum per-worker intervals; concurrent service occupancy is not wall
            # elapsed. Only gaps between steady calls on the same worker count.
            gaps = []
            for worker in range(selected["concurrency"]):
                owned = sorted(
                    (s for s in steady if s["worker"] == worker),
                    key=lambda s: s["slot_start_ns"],
                )
                gaps.extend(
                    b["slot_start_ns"] - a["slot_end_ns"]
                    for a, b in zip(owned, owned[1:])
                )
            costs = dict(
                preparation_ns=sum(s["start_ns"] - s["slot_start_ns"] for s in steady),
                validation_release_ns=sum(
                    s["slot_end_ns"] - s["resource_ns"] for s in steady
                ),
                owner_resource_ns=occupancy,
                dispatch_gap_ns=sum(gaps),
                dispatch_gap_count=len(gaps),
            )
            costs["slot_occupancy_ns"] = (
                costs["preparation_ns"] + occupancy + costs["validation_release_ns"]
            )
        resources, peak_rss, rss_reasons = rss_evidence(row, samples, first, last)
        resources = [
            r for r in resources if r["stage"] not in ("validated", "after_close")
        ]
        rows.append(
            dict(
                selected,
                input_retention_status=input_retention_status(row, n),
                input_retention=row.get("input_retention"),
                setup_heap_bytes=row["setup_heap_bytes"],
                throughput_scope=row.get(
                    "throughput_scope",
                    "owner/resource only; all inputs precreated and retained",
                ),
                p95_ns=durations[math.ceil(0.95 * len(durations)) - 1],
                responses_per_second=len(durations) * 1e9 / elapsed,
                wall_elapsed_ns=elapsed,
                owner_resource_occupancy_ns=occupancy,
                owner_service_rate_per_second=len(durations) * 1e9 / occupancy,
                steady_cost_sums=costs,
                peak_rss_bytes=peak_rss,
                rss_status="INCONCLUSIVE" if rss_reasons else "SAMPLED",
                rss_inconclusive_reasons=rss_reasons,
                fd_peak=max((r["fd"] for r in resources), default=None),
                thread_peak=max((r["threads"] for r in resources), default=None),
                goroutine_peak=max((r["goroutines"] for r in resources), default=None),
            )
        )
    provenance_paths = ["manifest.json", "rows/selection.json"] + [
        f"rows/row-{j:03d}.json" for j in range(len(selection))
    ]
    result = {
        "schema": 3,
        "provenance": {
            "source_bundle": str(out.resolve()),
            "source_sha256": {p: sha(out / p) for p in provenance_paths},
            "reducer_sha256": sha(Path(__file__)),
            "source_base_head": manifest.get("base_head"),
            "source_binary_sha256": manifest.get("binary_sha256"),
        },
        "method": "nearest-rank resource p95; disclosed closed-loop achieved throughput N/wall elapsed; separate owner service rate N/sum(resource_ns-start_ns), inverse mean service duration at matched fixed C, not achieved throughput; matched seed/workload/profile/C ratios; medians of paired ratios",
        "cost_scope": "steady samples only: summed preparation=start-slot_start; validation/release=slot_end-resource; dispatch gaps=next slot_start-previous slot_end on the same worker between steady calls only (excludes warmup transitions and worker edge idle time); sums are occupancy, not wall elapsed; no global wall subtraction; legacy rows without schema-2 slot timestamps have null costs",
        "units": "latency/elapsed/cost sums nanoseconds, memory bytes, achieved throughput responses/second, owner service rate inverse seconds; ratios unitless",
        "uncalibrated": True,
        "mode": manifest["mode"],
        "rows": rows,
        "pairs": [],
        "cells": [],
        "status": "SMOKE_ONLY_NO_PERFORMANCE_EVIDENCE",
    }
    index = {
        (r["seed"], r["workload"], r["profile"], r["concurrency"], r["lane"]): r
        for r in rows
    }
    for r in rows:
        if r["lane"] != "native":
            continue
        key = (r["seed"], r["workload"], r["profile"], r["concurrency"], "go")
        if key not in index:
            if manifest["mode"] == "campaign":
                raise ValueError("STOP: missing paired baseline")
            continue
        go = index[key]
        if r["throughput_scope"] != go["throughput_scope"]:
            raise ValueError("STOP: paired throughput scopes differ")
        result["pairs"].append(
            dict(
                seed=r["seed"],
                workload=r["workload"],
                profile=r["profile"],
                concurrency=r["concurrency"],
                p95=r["p95_ns"] / go["p95_ns"],
                throughput=r["responses_per_second"] / go["responses_per_second"],
                owner_service_rate=r["owner_service_rate_per_second"]
                / go["owner_service_rate_per_second"],
                memory=(
                    r["peak_rss_bytes"] / go["peak_rss_bytes"]
                    if r["peak_rss_bytes"] is not None
                    and go["peak_rss_bytes"] is not None
                    else None
                ),
                memory_conclusive=(
                    r["rss_status"] == go["rss_status"] == "SAMPLED"
                    and r["input_retention_status"]
                    == go["input_retention_status"]
                    == "BOUNDED_INPUTS"
                ),
            )
        )
    if manifest["mode"] == "campaign":
        if (
            len(rows) != 180
            or sum(r["count"] for r in rows) != 360000
            or len(selection) != 480
        ):
            raise ValueError("STOP: incomplete declared campaign")
        groups = {}
        for p in result["pairs"]:
            groups.setdefault(
                (p["workload"], p["profile"], p["concurrency"]), []
            ).append(p)
        for key, pairs in groups.items():
            if len(pairs) != 3 or {p["seed"] for p in pairs} != {26011, 26012, 26013}:
                raise ValueError("STOP: missing seed pair")
            cell = summarize_cell(key, pairs)
            result["cells"].append(cell)
        result["status"] = (
            "UNCALIBRATED_PASS"
            if len(result["cells"]) == 30
            and all(c["status"] == "PASS" for c in result["cells"])
            else "UNCALIBRATED_NOT_PASS"
        )
    # Exclusive output: never rewrite a historical derived result or raw bundle.
    with destination.open("x") as stream:
        stream.write(json.dumps(result, indent=2) + "\n")


def diagnostic_result(out):
    selection = json.loads((out / "rows/selection.json").read_text())
    row = json.loads((out / "rows/row-000.json").read_text())
    expected = dict(
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
    if (
        selection != [expected]
        or row.get("row") != expected
        or row.get("error")
        or row.get("stop_reason")
    ):
        raise ValueError("STOP: incorrect/failed diagnostic selection")
    if row.get("capture_budget") != DIAGNOSTIC_BUDGET:
        raise ValueError("STOP: incorrect diagnostic capture budget")
    samples, bounded, total = validate_samples(row, expected, out / "rows/row-000.json")
    # Comparative captures retain their default 120k eligibility contract.
    if (
        not bounded
        or row.get("resource_record_capacity") != 240000
        or input_retention_status(row, total, resource_capacity=240000)
        != "BOUNDED_INPUTS"
    ):
        raise ValueError("STOP: diagnostic input retention incomplete")
    steady = [s for s in samples if s["phase"] == "steady"]
    if (
        len(steady) != 5000
        or sum(s["phase"] == "first" for s in samples) != 1
        or sum(s["phase"] == "warmup" for s in samples) != 32
    ):
        raise ValueError("STOP: wrong diagnostic phase counts")
    first, last = min(s["slot_start_ns"] for s in steady), max(
        s["slot_end_ns"] for s in steady
    )
    resources, _, reasons = rss_evidence(row, samples, first, last)
    if len([r for r in resources if r["stage"] == "sample"]) > 240000:
        raise ValueError("STOP: diagnostic sampler limit exceeded")
    if any(not reason.startswith("sample: insufficient") for reason in reasons):
        raise ValueError(
            "STOP: incomplete diagnostic lifecycle resources: " + str(reasons)
        )
    write(
        out / "diagnostic.json",
        dict(
            status="DIAGNOSTIC_CAPTURE_COMPLETE_NO_PERFORMANCE_VERDICT",
            total_validated_closed_slots=total,
            steady_requests=len(steady),
            row_elapsed_ns=max(s["slot_end_ns"] for s in samples),
            resource_records=len(resources),
            rss_status="INCONCLUSIVE" if reasons else "SAMPLED",
            rss_inconclusive_reasons=reasons,
            input_retention=row["input_retention"],
            capture_budget=row["capture_budget"],
            provenance=dict(
                raw_sha256=sha(out / "rows/row-000.json"),
                binary_sha256=json.loads((out / "manifest.json").read_text())[
                    "binary_sha256"
                ],
                reducer_sha256=sha(Path(__file__)),
            ),
        ),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    run = sub.add_parser("capture")
    run.add_argument(
        "--mode", choices=["plan", "smoke", "campaign", "diagnostic"], required=True
    )
    run.add_argument("--binary", required=True)
    run.add_argument("--out", required=True)
    run.add_argument("--go", default="/home/ashutosh/.cache/go1.26.4/go/bin/go")
    for key in ["lanes", "profiles", "workloads", "concurrency"]:
        run.add_argument("--" + key)
    calc = sub.add_parser("derive")
    calc.add_argument("out", type=Path)
    calc.add_argument(
        "--output",
        type=Path,
        required=True,
        help="new derived JSON path; never overwrite an earlier result",
    )
    args = parser.parse_args()
    try:
        (
            capture(args)
            if args.action == "capture"
            else derive(args.out.resolve(), args.output)
        )
    except (ValueError, OSError, KeyError) as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
