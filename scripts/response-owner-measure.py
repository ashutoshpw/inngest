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
import time
from datetime import datetime, timezone


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write(path, data):
    # Preserve the previous complete record if a replacement write fails.
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        stream.write(json.dumps(data, indent=2) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


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
SCREEN_BUDGET = dict(
    post_setup_watchdog_ns=60_000_000_000,
    row_context_ns=90_000_000_000,
    test_timeout_ns=110_000_000_000,
    outer_kill_ns=120_000_000_000,
    request_deadline_ns=20_000_000_000,
    resource_records=120000,
)
SCREEN_SELECTORS = dict(
    lanes="go,native",
    profiles="controlled",
    workloads="run64,mixed8,run64k",
    concurrency="1",
)
SCREEN_SECONDS = 900
# Control reserve INSIDE the package budget; no measurement or retry in it.
SCREEN_SHUTDOWN_GRACE = 6
SCREEN_SEAL_RESERVE = 4


def screen_selectors(args):
    for key, value in SCREEN_SELECTORS.items():
        if getattr(args, key) not in (None, value):
            raise ValueError(
                "screen requires exact go,native/controlled/run64,mixed8,run64k/C1 selectors"
            )
        setattr(args, key, value)


class ScreenPackage:
    """One monotonic budget from capture entry through final bookkeeping."""

    def __init__(self):
        self.start = time.monotonic()
        self.out = None
        self.process = None

    def remaining(self):
        remaining = (SCREEN_SECONDS - SCREEN_SHUTDOWN_GRACE - SCREEN_SEAL_RESERVE
                     - (time.monotonic() - self.start))
        if remaining <= 0:
            raise ValueError("STOP: screen package wall budget expired (shutdown reserve)")
        return remaining

    def hard_remaining(self):
        remaining = SCREEN_SECONDS - (time.monotonic() - self.start)
        if remaining <= 0:
            self.hard_expired()
        return remaining

    def terminate(self):
        process = self.process
        if process is None:
            return None
        result = dict(request="SIGUSR1", status="cooperative_exit")
        # Include at most .5s for hard-kill reaping inside the control grace.
        grace = max(.001, min(SCREEN_SHUTDOWN_GRACE - .5,
                              self.hard_remaining() - SCREEN_SEAL_RESERVE - .5))
        stop_by = time.monotonic() + grace
        try:
            os.killpg(process.pid, signal.SIGUSR1)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            result.update(status="flush_unconfirmed_hard_kill", no_verdict=True)
        # The direct parent can die first (e.g. Go test timeout). Its wait then
        # says nothing about a surviving native descendant's diagnostic flush.
        descendants_unconfirmed = False
        while process.poll() is not None and time.monotonic() < stop_by:
            try:
                os.killpg(process.pid, 0)
                descendants_unconfirmed = True
            except ProcessLookupError:
                descendants_unconfirmed = False
                break
            time.sleep(min(.05, max(.001, stop_by - time.monotonic())))
        if descendants_unconfirmed:
            result.update(status="flush_unconfirmed_hard_kill", no_verdict=True)
        # A parent may have exited while a native descendant is still alive.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=min(.5, self.hard_remaining()))
        self.process = None
        return result

    def expired(self, *_):
        raise ValueError("STOP: screen package wall budget expired (shutdown reserve)")

    def hard_expired(self, *_):
        if self.process is not None:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        raise ValueError("STOP: package cleanup budget exhausted; diagnostic flush/seal unconfirmed; no verdict")


def artifact_ledger(out):
    write(
        out / "artifacts.json",
        {
            str(p.relative_to(out)): sha(p)
            for p in sorted(out.rglob("*"))
            if p.is_file() and p.name not in ("artifacts.json", "CAPTURE_INCOMPLETE")
        },
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
    if getattr(args, "declare_only", False) and args.mode != "screen":
        raise ValueError("declare-only requires screen mode")
    if args.mode != "screen":
        return capture_bundle(args)
    package = ScreenPackage()
    screen_selectors(args)
    # Linux capture host: the alarm also bounds source archiving, version
    # commands, derivation and hashing, rather than just the measurement child.
    previous = signal.signal(signal.SIGALRM, package.expired)
    signal.setitimer(signal.ITIMER_REAL, package.remaining())
    try:
        capture_bundle(args, package)
    except BaseException as exc:
        print(f"STOP: capture failed: {exc}; preserving incomplete bundle", file=sys.stderr)
        # Failure cleanup shares the original hard deadline, including sealing.
        signal.signal(signal.SIGALRM, package.hard_expired)
        signal.setitimer(signal.ITIMER_REAL, package.hard_remaining())
        stop = package.terminate()
        if package.out is not None:
            # Invalidate derived output before any further artifact write.
            if (package.out / "derived.json").exists():
                (package.out / "derived.json").rename(package.out / "unaccepted-derived.json")
            # Failure sealing consumes only the reserved control budget.
            path = package.out / "manifest.json"
            try:
                manifest = json.loads(path.read_text()) if path.exists() else {}
            except (OSError, ValueError):
                # An alarm can interrupt the manifest write itself. Preserve
                # those bytes as well as the original stop reason.
                if path.exists():
                    path.rename(package.out / "interrupted-manifest.json")
                manifest = {"interrupted_manifest": True}
            manifest.update(
                mode="screen",
                stop_reason=str(exc),
                shutdown=stop,
                package_elapsed_seconds=time.monotonic() - package.start,
            )
            write(path, manifest)
            artifact_ledger(package.out)
        raise
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def capture_bundle(args, package=None):
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
    # Removed only after all final writes/checks succeed; failed sealing must
    # never leave apparently accepted rows/manifest eligible for derivation.
    (out / "CAPTURE_INCOMPLETE").write_text("STARTED; no accepted verdict\n")
    if package:
        package.out = out
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
        "child_evidence_required": args.mode != "diagnostic",
        "capture_argv": sys.argv,
        "reducer_sha256": sha(Path(__file__)),
        "diagnostic_budget": DIAGNOSTIC_BUDGET if args.mode == "diagnostic" else None,
        **(
            dict(
                screen_budget=SCREEN_BUDGET,
                package_budget_seconds=SCREEN_SECONDS,
                declaration_only=args.declare_only,
                shutdown_grace_seconds=SCREEN_SHUTDOWN_GRACE,
                seal_reserve_seconds=SCREEN_SEAL_RESERVE,
            )
            if package else {}
        ),
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
    if package:
        manifest["policy"] += (
            "; FIXED SCREEN: complete all 18 rows/three seeds; independent rejection "
            "survives memory INCONCLUSIVE; no overall PASS, transport choice or CPU-consumption claim"
        )
    write(out / "manifest.json", manifest)
    watched = [root / name for name in sources] + [binary, native]
    signatures = {
        p: (p.stat().st_mtime_ns, p.stat().st_size, p.stat().st_ino)
        for p in watched
    } if package else {}
    if package and any(sha(root / name) != digest for name, digest in sources.items()):
        raise ValueError("STOP: provenance changed during screen setup")
    env = dict(os.environ, GOMAXPROCS="8")
    env.pop("RESPONSE_MEASURE_CHILD", None)
    env.pop("RESPONSE_MEASURE_ROW_PATH", None)
    for argv in [
        [args.go, "version"],
        ["rustc", "+1.94.0", "--version"],
        ["cc", "--version"],
    ]:
        r = subprocess.run(
            argv, capture_output=True, text=True, cwd=root,
            **(dict(timeout=package.remaining()) if package else {}),
        )
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
    if package:
        remaining = package.remaining()
        argv[3] = f"-test.timeout={remaining + SCREEN_SHUTDOWN_GRACE:.9f}s"
        argv.append(f"-response-measure-package-remaining={remaining:.9f}s")
        if args.declare_only:
            argv.append("-response-measure-declare-only")
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
        "status": "STARTED",
    }
    manifest["commands"].append(command)
    write(out / "manifest.json", manifest)
    with open(out / "run.log", "wb", buffering=0) as log:
        if package:
            process = subprocess.Popen(
                argv, cwd=command["cwd"], env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True,
            )
            package.process = process
            os.set_blocking(process.stdout.fileno(), False)

            def drain_output():
                # Stream bounded chunks and observe actual filesystem errors;
                # passing an FD straight to the child cannot report its writes.
                while True:
                    try:
                        chunk = os.read(process.stdout.fileno(), 65536)
                    except BlockingIOError:
                        return False
                    if not chunk:
                        return True
                    if log.write(chunk) != len(chunk):
                        raise OSError("short run.log write; capture incomplete")

            try:
                next_provenance = time.monotonic()
                while process.poll() is None:
                    try:
                        process.wait(timeout=min(0.25, package.remaining()))
                    except subprocess.TimeoutExpired:
                        pass
                    drain_output()
                    if time.monotonic() >= next_provenance:
                        next_provenance = time.monotonic() + 1
                        for p, signature in signatures.items():
                            current = p.stat()
                            if (current.st_mtime_ns, current.st_size, current.st_ino) != signature:
                                raise ValueError(f"STOP: provenance changed during screen: {p}")
                        current_names = set(subprocess.check_output(
                            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                            cwd=root, timeout=package.remaining()).split(b"\0")) - {b""}
                        if current_names != set(names):
                            raise ValueError("STOP: source path set changed during screen")
                if not drain_output():
                    raise ValueError("STOP: child output pipe incomplete after parent exit")
                r = process
            finally:
                failure = sys.exc_info()[1]
                if failure is not None:
                    signal.signal(signal.SIGALRM, package.hard_expired)
                    signal.setitimer(signal.ITIMER_REAL, package.hard_remaining())
                    command["stop_reason"] = str(failure)
                    command["shutdown"] = package.terminate()
                elif process.returncode:
                    command["shutdown"] = package.terminate()
                else:
                    package.process = None
                output_error = None
                try:
                    if not drain_output():
                        raise ValueError("STOP: child output flush unconfirmed; no verdict")
                    log.flush()
                    os.fsync(log.fileno())
                except (OSError, ValueError) as exc:
                    output_error = exc
                    command["output_error"] = str(exc)
                finally:
                    process.stdout.close()
                command.update(exit=process.returncode,
                               status="INCOMPLETE" if failure or output_error or process.returncode else "COMPLETE",
                               end_utc=datetime.now(timezone.utc).isoformat())
                write(out / "manifest.json", manifest)
                if output_error is not None and failure is None:
                    raise output_error
        elif args.mode == "diagnostic":
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
    command.update(exit=r.returncode, status="COMPLETE" if r.returncode == 0 else "INCOMPLETE", end_utc=datetime.now(timezone.utc).isoformat())
    manifest["host_after"] = readable_host()
    changed = [
        name
        for name, digest in sources.items()
        if not (root / name).is_file() or sha(root / name) != digest
    ]
    if package:
        current_names = set(subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=root, timeout=package.remaining()).split(b"\0")) - {b""}
        changed += [os.fsdecode(n) for n in current_names - set(names)]
    manifest.update(
        source_changed=changed,
        binary_changed=sha(binary) != manifest["binary_sha256"],
        native_changed=sha(native) != manifest["native_static_sha256"],
    )
    if package:
        package.remaining()
        # Snapshot before derivation. The alarm remains armed through both the
        # reducer and final ledger; any expiry invalidates and seals the bundle.
        manifest["package_elapsed_seconds"] = time.monotonic() - package.start
    write(out / "manifest.json", manifest)
    # Hash raw logs/commands/rows even on failure; no favorable-only filtering.
    artifact_ledger(out)
    if (
        r.returncode
        or changed
        or manifest["binary_changed"]
        or manifest["native_changed"]
    ):
        raise ValueError(f"STOP: execution/source failure; preserve {out}")
    if args.mode == "diagnostic":
        diagnostic_result(out)
    elif args.mode != "plan" and not (package and args.declare_only):
        derive(out, out / "derived.json", capturing=True)
    artifact_ledger(out)
    if package:
        package.remaining()
    (out / "CAPTURE_INCOMPLETE").unlink()
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


def screen_selection(selection):
    """Validate the complete fixed declaration before deriving any cell."""
    expected = {
        (seed, workload, lane)
        for seed in (26011, 26012, 26013)
        for workload in ("run64", "mixed8", "run64k")
        for lane in ("go", "native")
    }
    if len(selection) != 18:
        raise ValueError("STOP: incomplete declared screen (18 rows required)")
    seen = set()
    for j, row in enumerate(selection):
        key = (row.get("seed"), row.get("workload"), row.get("lane"))
        if (
            key not in expected
            or key in seen
            or row != dict(
                preflight=False, seed=key[0], workload=key[1], lane=key[2],
                profile="controlled", concurrency=1, count=500,
                warmups_per_slot=32, first_only=False, first_pair=0, order=j,
            )
            or row["seed"] != (26011, 26012, 26013)[j // 6]
        ):
            raise ValueError("STOP: wrong/duplicate fixed screen row")
        seen.add(key)
    for a, b in zip(selection[::2], selection[1::2]):
        if (
            (a["seed"], a["workload"]) != (b["seed"], b["workload"])
            or a["lane"] == b["lane"]
        ):
            raise ValueError("STOP: screen lanes must be adjacent paired cells")
    return selection


def validate_screen_raw(row, selected, path):
    samples, bounded, total = validate_samples(row, selected, path)
    if (
        not bounded or row.get("stop_reason")
        or row.get("capture_budget") != SCREEN_BUDGET
        or row.get("rss_interval_ns") != 1_000_000
    ):
        raise ValueError(f"STOP: wrong/failed screen capture controls {path}")
    for sample in samples:
        ident = sample["id"]
        phase = "first" if ident == 0 else ("warmup" if ident <= selected["warmups_per_slot"] else "steady")
        if sample["phase"] != phase or sample["worker"] != 0:
            raise ValueError(f"STOP: wrong screen phase/worker {path}")
    if len([r for r in row.get("resources", []) if r.get("stage") == "sample"]) > 120000:
        raise ValueError(f"STOP: screen sampler cap exceeded {path}")
    # Missing lifecycle/retention evidence prevents a completed screen. Sparse
    # sampling alone remains a memory-only INCONCLUSIVE result.
    if input_retention_status(row, total) != "BOUNDED_INPUTS":
        raise ValueError(f"STOP: screen input retention incomplete {path}")
    if selected["count"]:
        steady = [s for s in samples if s["phase"] == "steady"]
        first, last = min(s["slot_start_ns"] for s in steady), max(s["slot_end_ns"] for s in steady)
        _, _, reasons = rss_evidence(row, samples, first, last)
        if any(not reason.startswith("sample: insufficient") for reason in reasons):
            raise ValueError(f"STOP: incomplete screen lifecycle resources {path}: {reasons}")


def validate_child_evidence(path):
    command_path = path.with_name(path.stem + "-command.json")
    log_path = path.with_suffix(".log")
    try:
        command = json.loads(command_path.read_text())
        if (command.get("status") != "COMPLETE" or command.get("error") != "<nil>"
                or not command.get("argv") or not command.get("start_utc")
                or not command.get("end_utc") or not log_path.is_file()):
            raise ValueError("incomplete command/log")
        # Opening both artifacts rejects unreadable evidence as well as absence.
        with log_path.open("rb") as stream:
            stream.read(1)
    except (OSError, ValueError) as exc:
        raise ValueError(f"STOP: missing/failed child command/log evidence {path}: {exc}") from exc


def validate_screen_bundle(out, manifest, selection):
    screen_selection(selection)
    elapsed = manifest.get("package_elapsed_seconds")
    if (
        manifest.get("declaration_only") or manifest.get("stop_reason")
        or manifest.get("screen_budget") != SCREEN_BUDGET
        or manifest.get("package_budget_seconds") != SCREEN_SECONDS
        or manifest.get("shutdown_grace_seconds") != SCREEN_SHUTDOWN_GRACE
        or manifest.get("seal_reserve_seconds") != SCREEN_SEAL_RESERVE
        or not usable_rss(elapsed) or elapsed > SCREEN_SECONDS
        or not manifest.get("commands")
    ):
        raise ValueError("STOP: incomplete/expired screen package")
    validate_child_evidence(out / "rows/preflight.json")
    preflight = json.loads((out / "rows/preflight-command.json").read_text())
    if preflight.get("error") != "<nil>" or not (out / "rows/preflight.log").is_file():
        raise ValueError("STOP: failed/missing screen functional preflight")
    seen = set()
    for selected in selection:
        key = (selected["workload"], selected["lane"])
        if key in seen:
            continue
        j = len(seen)
        seen.add(key)
        port = dict(selected, preflight=True, count=0, warmups_per_slot=0,
                    first_only=True, first_pair=0, order=j)
        path = out / f"rows/port-preflight-{j:03d}.json"
        validate_child_evidence(path)
        raw = json.loads(path.read_text())
        if raw.get("row") != port or raw.get("error"):
            raise ValueError(f"STOP: wrong/failed screen port preflight {path}")
        validate_screen_raw(raw, port, path)


def screen_decision(cell):
    # Aggregate memory status deliberately keeps its existing meaning; a valid
    # latency/rate rejection must not disappear behind sparse RSS evidence.
    if any(cell[m]["status"] == "FAIL" for m in ("p95", "throughput", "owner_service_rate", "memory")):
        return "NO_GO"
    if cell["status"] == "PASS":
        return "PROMISING_NEEDS_BROADER_GATES"
    return "INCONCLUSIVE"


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


def derive(out, destination, *, capturing=False):
    destination = destination.resolve()
    if destination.exists():
        raise ValueError("STOP: derived destination already exists; choose a new path")
    if (out / "CAPTURE_INCOMPLETE").exists() and not capturing:
        raise ValueError("STOP: incomplete capture/failure seal; no verdict")
    manifest = json.loads((out / "manifest.json").read_text())
    if manifest["mode"] == "diagnostic":
        raise ValueError(
            "STOP: diagnostic capture has no comparative derivation or campaign verdict"
        )
    if (
        manifest.get("stop_reason")
        or any(c.get("exit", 1) != 0 for c in manifest["commands"])
        or manifest.get("source_changed")
        or manifest.get("binary_changed")
        or manifest.get("native_changed")
    ):
        raise ValueError("STOP: failed command or changed source/binary")
    selection = json.loads((out / "rows/selection.json").read_text())
    if manifest["mode"] == "screen":
        validate_screen_bundle(out, manifest, selection)
    if manifest.get("child_evidence_required") and manifest["mode"] != "screen":
        validate_child_evidence(out / "rows/preflight.json")
        seen = set()
        for selected in selection:
            key = (selected["profile"], selected["workload"], selected["lane"])
            if key not in seen:
                validate_child_evidence(out / f"rows/port-preflight-{len(seen):03d}.json")
                seen.add(key)
    rows = []
    for j, selected in enumerate(selection):
        p = out / f"rows/row-{j:03d}.json"
        if manifest["mode"] == "screen" or manifest.get("child_evidence_required"):
            validate_child_evidence(p)
        row = json.loads(p.read_text())
        if row.get("error") or row["row"] != selected:
            raise ValueError(f"STOP: wrong/missing/contaminated row {p}")
        samples, bounded, n = validate_samples(row, selected, p)
        if manifest["mode"] == "screen":
            validate_screen_raw(row, selected, p)
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
            if manifest["mode"] in ("campaign", "screen"):
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
    if manifest["mode"] in ("campaign", "screen"):
        if manifest["mode"] == "campaign" and (
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
            if manifest["mode"] == "screen":
                cell["screen_decision"] = screen_decision(cell)
            result["cells"].append(cell)
        result["status"] = (
            "UNCALIBRATED_PASS"
            if len(result["cells"]) == 30
            and all(c["status"] == "PASS" for c in result["cells"])
            else "UNCALIBRATED_NOT_PASS"
        )
        if manifest["mode"] == "screen":
            decisions = [c["screen_decision"] for c in result["cells"]]
            result["screen_decision"] = (
                "NO_GO" if "NO_GO" in decisions else
                "PROMISING_NEEDS_BROADER_GATES" if all(d == "PROMISING_NEEDS_BROADER_GATES" for d in decisions) else
                "INCONCLUSIVE"
            )
            result["status"] = "SCREEN_" + result["screen_decision"]
            result["unmeasured"] = ["C8", "Valkey", "retryable", "enqueue-error", "persistent-process", "native-arm64"]
            result["limits"] = "Fixed rejection screen only; no overall PASS, transport choice, activation or CPU-consumption claim; service rate is inverse mean wall occupancy."
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
        "--mode", choices=["plan", "smoke", "campaign", "screen", "diagnostic"], required=True
    )
    run.add_argument("--declare-only", action="store_true", help="screen declaration only; execute no preflights or rows")
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
    except (ValueError, OSError, KeyError, subprocess.TimeoutExpired) as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
