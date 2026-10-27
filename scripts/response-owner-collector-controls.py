"""One <=30s collector-only package: one launcher, three sequential children."""
import time
START_NS = time.monotonic_ns()
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys


def child(mode):
    signal.alarm(10)
    print(json.dumps(dict(event="ready", pid=os.getpid(), uid=os.getuid(), gid=os.getgid())), flush=True)
    command = sys.stdin.readline().strip()
    if command == "deny" and mode == "live_nondumpable":
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(4, 0, 0, 0, 0) != 0:  # PR_SET_DUMPABLE, this child only.
            raise OSError(ctypes.get_errno(), "PR_SET_DUMPABLE")
        print(json.dumps(dict(event="nondumpable", dumpable=libc.prctl(3, 0, 0, 0, 0))), flush=True)
        command = sys.stdin.readline().strip()
    if command != "exit":
        raise ValueError("unexpected child command")


def launch(out):
    out.mkdir()  # Exclusive: never overwrite or retry an earlier package.
    spec = importlib.util.spec_from_file_location("collector", Path(__file__).with_name("response-owner-oneshot.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    receipt = dict(start_ns=START_NS, uid=os.getuid(), gid=os.getgid(), launcher_pid=os.getpid(),
                   launcher_count=1, child_launches=0, owner_launches=0, store_operations=0,
                   controls=[], status="STOPPED", memory_status="INCONCLUSIVE")
    deadline = START_NS / 1e9 + 25

    def remaining():
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("synthetic work budget exhausted; no next dispatch")
        return min(1.0, left)

    def line(process, item):
        if not select.select([process.stdout], [], [], remaining())[0]:
            raise TimeoutError("synthetic child acknowledgement")
        raw = process.stdout.readline(4097)
        item.setdefault("stdout", []).append(raw.decode(errors="replace"))
        if not raw or len(raw) > 4096:
            raise RuntimeError("missing/oversized child acknowledgement")
        return json.loads(raw)

    def sample(process, item, expected=None):
        try:
            r = m.proc_record(process.pid, "synthetic_child", expected)
        except m.ProcSampleError as exc:
            item.setdefault("samples", []).append(exc.record)
            raise
        item.setdefault("samples", []).append(r)
        return r

    def expired(*_):
        raise TimeoutError("synthetic package hard budget")

    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, max(.001, START_NS / 1e9 + 29 - time.monotonic()))
    try:
        if os.getuid() == 0 or os.geteuid() != os.getuid():
            raise RuntimeError("same-UID ordinary runner required")
        for mode in ("live_readable", "normal_exit_unreaped", "live_nondumpable"):
            remaining()
            item = dict(mode=mode, start_ns=time.monotonic_ns(), status="STOPPED")
            receipt["controls"].append(item)
            process = None
            try:
                argv = [sys.executable, str(Path(__file__).resolve()), "--child", mode]
                item["argv"] = argv
                process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT, bufsize=0)
                receipt["child_launches"] += 1
                item["pid"] = process.pid
                ready = line(process, item)
                if ready != dict(event="ready", pid=process.pid, uid=os.getuid(), gid=os.getgid()):
                    raise RuntimeError("child identity acknowledgement mismatch")
                before = sample(process, item)
                if before["rss_kib"] <= 0 or before["fds"] is None:
                    raise RuntimeError("live readable control missing metrics")
                ticks = before["start_ticks"]
                if mode == "normal_exit_unreaped":
                    process.stdin.write(b"exit\n")
                    # WNOWAIT independently establishes exit without reaping the child.
                    while True:
                        remaining()
                        waited = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                        if waited is not None:
                            break
                        time.sleep(.001)
                    item["waitid"] = dict(pid=waited.si_pid, code=waited.si_code, status=waited.si_status,
                                          observed_ns=time.monotonic_ns(), unreaped=True)
                    after = sample(process, item, ticks)
                    if not after["exit_established"] or after["rss_kib"] is not None:
                        raise RuntimeError("unreaped exit not established or memory fabricated")
                elif mode == "live_nondumpable":
                    process.stdin.write(b"deny\n")
                    if line(process, item) != dict(event="nondumpable", dumpable=0):
                        raise RuntimeError("nondumpable acknowledgement missing")
                    try:
                        sample(process, item, ticks)
                    except m.ProcSampleError as exc:
                        if (exc.record["fields"]["fds"]["errno"] != 13
                                or exc.record["exit_established"] or not exc.record["identity_verified"]
                                or exc.record["rss_kib"] is None):
                            raise RuntimeError("live denial did not exercise retained EACCES") from exc
                        item["expected_fail_stop"] = str(exc)
                    else:
                        raise RuntimeError("nondumpable child unexpectedly readable")
                item["status"] = "PASS"
            except BaseException as exc:
                item["error"] = repr(exc)
                raise
            finally:
                if process is not None:
                    try:
                        if process.returncode is None:
                            try:
                                process.stdin.write(b"exit\n")
                            except BrokenPipeError:
                                pass
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        item["hard_kill"] = True
                        process.kill()
                        process.wait(timeout=1)
                    finally:
                        process.stdin.close()
                        process.stdout.close()
                    item["exit"] = process.returncode
                    try:
                        os.kill(process.pid, 0)
                    except ProcessLookupError:
                        item["confirmed_absent"] = True
                    else:
                        raise RuntimeError("synthetic child remains")
                item["end_ns"] = time.monotonic_ns()
                m.write(out / (mode + ".json"), item)
        receipt["status"] = "COMPLETE_CONTROLS_ONLY"
    except BaseException as exc:
        receipt["error"] = repr(exc)
    finally:
        receipt["end_ns"] = time.monotonic_ns()
        receipt["elapsed_seconds"] = (receipt["end_ns"] - START_NS) / 1e9
        receipt["within_30_seconds"] = receipt["elapsed_seconds"] <= 30
        receipt["artifacts"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir() if p.is_file()}
        m.write(out / "receipt.json", receipt)
        signal.setitimer(signal.ITIMER_REAL, 0)
    print(json.dumps(receipt))
    return 0 if receipt["status"] == "COMPLETE_CONTROLS_ONLY" and receipt["within_30_seconds"] else 1


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--child":
        child(sys.argv[2])
    elif len(sys.argv) == 2:
        sys.exit(launch(Path(sys.argv[1])))
    else:
        raise SystemExit("usage: response-owner-collector-controls.py NEW_OUTPUT_DIRECTORY")
