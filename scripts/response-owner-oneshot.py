#!/usr/bin/env python3
"""Reduced constructed-domain readiness. No production/language baseline.

`declare` and synthetic `reduce` do not invoke owners. `capture` is FUTURE ONLY:
separate parent authorization after independent Luna + Sol fairness review is
required. The old FFI driver, selector and reducer are deliberately not imported.
"""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tarfile
import time
import uuid
import re

SEEDS = (26011, 26012, 26013)
PAYLOADS = (64, 65536)
PACKAGE_SECONDS = 180
STOP_SECONDS = 6
SEAL_SECONDS = 4
RESOURCE_CAP = 120000
POLL_SECONDS = .005  # requested interval; never a scheduling guarantee
REVIEW_ACCEPTANCE = "parent-accepted:readinessreview03+clockaccountingreview02+P3precedencereview01+runnerstopreview01+declarationreview02"
STATE_PATH = Path(__file__).resolve().parents[1] / "pkg/execution/executor/testdata/response-oneshot-authorization.json"


def strict_authorization(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate authorization field")
            result[key] = value
        return result
    if len(raw) > 4096:
        raise ValueError("authorization capacity")
    return json.loads(raw, object_pairs_hook=unique)


def authorization_state():
    return strict_authorization(STATE_PATH.read_text())


def validate_authorization(state, requested, out, now=None):
    """Source-owned allowlist, not an externally asserted acceptance boolean."""
    if (set(state) != {"schema", "review_acceptance", "authorization"}
            or type(state["schema"]) is not int or state["schema"] != 1 or state["review_acceptance"] != REVIEW_ACCEPTANCE):
        raise ValueError("unrecognized review acceptance")
    grant = state["authorization"]
    if not isinstance(grant, dict) or set(grant) != {
            "id", "scope", "declaration_sha256", "out", "claim", "expires_unix", "attempts"}:
        raise ValueError("capture requires separate parent authorization; none issued")
    if (not isinstance(grant["id"], str) or not re.fullmatch(r"[a-z0-9-]{16,80}", grant["id"])
            or requested != grant["id"] or grant["scope"] != "oneshot-capability-60-plus-8"
            or grant["declaration_sha256"] != sha(STATE_PATH.with_name("response-oneshot-declaration.json"))
            or type(grant["attempts"]) is not int or grant["attempts"] != 1
            or type(grant["expires_unix"]) is not int
            or grant["expires_unix"] <= (time.time() if now is None else now)
            or not isinstance(grant["out"], str) or not Path(grant["out"]).is_absolute()
            or str(Path(out).resolve()) != grant["out"]
            or not isinstance(grant["claim"], str) or not Path(grant["claim"]).is_absolute()
            or Path(grant["claim"]).is_relative_to(Path(grant["out"]))):
        raise ValueError("capture authorization expired or scope mismatch")
    return grant


def claim_authorization(grant):
    # A scope stop or any later failure consumes the attempt. Never unlink/retry.
    with Path(grant["claim"]).open("x") as f:
        json.dump(grant, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())


# Concrete readiness build contract: existing reviewed profiles, no custom flags.
BUILD_SETTINGS = {
    "go": "default optimized test build; no custom gcflags/ldflags",
    "go_race": "default optimized race test build; no custom gcflags/ldflags",
    "native": "release; repository Cargo profile (panic=unwind)",
    "private_native": "debug test; default Cargo profile",
}
# Credentials remain in the existing HOME/DOCKER_CONFIG files. Never copy raw
# credential variables (or arbitrary RESPONSE_*, GO*, RUST*, LD_* flags).
ENV_PATHS = ("PATH", "HOME", "TMPDIR", "GOTMPDIR", "XDG_RUNTIME_DIR",
             "DOCKER_CONFIG", "DOCKER_HOST", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH",
             "CARGO_HOME", "RUSTUP_HOME", "SSL_CERT_FILE", "SSL_CERT_DIR")
ENV_FIXED = dict(GOMAXPROCS="2", GOTOOLCHAIN="local", GOENV="off",
                 GOFLAGS="", GODEBUG="", EXPERIMENTAL_KEY_QUEUES_ENABLE="false")


def execution_environment(parent=None):
    parent = os.environ if parent is None else parent
    env = {k: parent[k] for k in ENV_PATHS if k in parent}
    # Docker's supported unix/tcp endpoints need no embedded credentials.
    if "@" in env.get("DOCKER_HOST", "") or "?" in env.get("DOCKER_HOST", ""):
        raise ValueError("credential-bearing Docker endpoint is not supported; use secure config")
    env.update(ENV_FIXED)
    return env


def build_configuration(root, env):
    # Cargo searches cwd ancestors and CARGO_HOME, outside the source manifest.
    homes = [p / ".cargo" for p in (Path(root).resolve(), *Path(root).resolve().parents)]
    homes.append(Path(env.get("CARGO_HOME", str(Path(env["HOME"]) / ".cargo"))))
    if any((home / name).exists() for home in homes for name in ("config", "config.toml")):
        raise RuntimeError("STOP: external Cargo build configuration is not accepted")
    return dict(cargo_config="absent", goenv="off", cargo_profile="repository source manifest")


def validate_build_contract(build, sources, binary, native, go, root):
    """Validate recorded effective commands, not prose or mere flag presence."""
    def reject():
        raise RuntimeError("STOP: build/source/optimization provenance mismatch")

    if (build.get("source_code_sha256") != code_digest(sources)
            or build.get("go_binary_sha256") != sha(binary)
            or build.get("native_binary_sha256") != sha(native)
            or build.get("go_version") != "go1.26.4"
            or build.get("rust_version") != "1.94.0"
            or build.get("optimization") != BUILD_SETTINGS
            or build.get("build_configuration") != build_configuration(root, execution_environment())):
        reject()
    commands = build.get("build_commands")
    if not isinstance(commands, dict) or set(commands) != {"go", "go_race", "native", "private_native"}:
        reject()
    for role, command_receipt in commands.items():
        if not isinstance(command_receipt, dict):
            reject()
        argv = command_receipt.get("argv")
        env = command_receipt.get("environment")
        if (not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items())
                or not env.get("HOME") or not env.get("PATH") or env != execution_environment(env)
                or command_receipt.get("exit") != 0
                or command_receipt.get("cwd") != str(Path(root).resolve())
                or not isinstance(argv, list) or not all(isinstance(a, str) for a in argv)):
            reject()
        if build_configuration(root, env) != build["build_configuration"]:
            reject()
        if role in ("go", "go_race"):
            # Outputs may live in any external artifact directory; no flag or
            # package substitution is permitted around those path operands.
            if len(argv) != (9 if role == "go_race" else 8):
                reject()
            expected = [str(Path(go).resolve()), "test", "-p=2", "-c"]
            if role == "go_race":
                expected.append("-race")
            expected += ["-tags=native_step_integration", "-o", argv[-2], "./pkg/execution/executor"]
            if argv != expected or not Path(argv[-2]).is_absolute():
                reject()
            if role == "go" and (Path(argv[-2]).resolve() != Path(binary).resolve()
                    or build.get("argv") != argv or build.get("environment") != env):
                reject()
        else:
            expected = ["cargo", "+1.94.0", "build" if role == "native" else "test",
                        "--manifest-path", "rust/Cargo.toml", "-p", "native-step-state",
                        "--features", "test-helper"]
            expected += (["--release", "-j2", "--bin", "native-step-test-helper"]
                         if role == "native" else ["-j2", "--lib", "--no-run"])
            if argv != expected:
                reject()


def declaration():
    # One reviewed literal contract is consumed by BOTH entries. Tests separately
    # prove its grid, pairing, phases, control bounds and the fixed seeded order.
    path = Path(__file__).resolve().parents[1] / "pkg/execution/executor/testdata/response-oneshot-declaration.json"
    return json.loads(path.read_text())


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def code_digest(sources):
    # Bind every source/asset/lock/script, including embedded HTML/SQL/etc.
    # Only this increment's three non-executable task records may be finalized
    # after validation. The full capture manifest still pins those records too.
    task = ".agents/tasks/2026-10-26/01-rust-core-migration/"
    evidence_docs = {task + "plan.md", task + "status.md",
                     task + "references/response-owner-oneshot-readiness.md"}
    code = {p: digest for p, digest in sources.items()
            if p not in evidence_docs}
    return hashlib.sha256(json.dumps(code, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def write(path, obj):
    # Reuse the reviewed atomic checked-write pattern. Best effort under an OS
    # failure; SIGKILL, full media and power loss cannot guarantee a sealed file.
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w") as f:
        json.dump(obj, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


class Budget:
    def __init__(self, clock=time.monotonic):
        self.clock, self.start = clock, clock()
        self.process = None

    def remaining(self, reserve=STOP_SECONDS + SEAL_SECONDS):
        left = PACKAGE_SECONDS - reserve - (self.clock() - self.start)
        if left <= 0:
            raise RuntimeError("STOP: package deadline, including stop/sealing reserve")
        return left


class Stream:
    """Bounded streamed evidence, no retained resource/payload array."""
    def __init__(self, path, cap=RESOURCE_CAP):
        self.file = Path(path).open("x")
        self.cap, self.count = cap, 0

    def emit(self, value):
        if self.count >= self.cap:
            raise RuntimeError("STOP: evidence retention capacity")
        encoded = json.dumps(value, separators=(",", ":")) + "\n"
        if len(encoded) > 1024 * 1024:
            raise RuntimeError("STOP: evidence record size")
        if self.file.write(encoded) != len(encoded):
            raise OSError("short artifact write")
        self.file.flush()
        self.count += 1

    def close(self):
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()


class ProcSampleError(RuntimeError):
    """Fail-stop with the failed sample available to receipt sealers."""
    def __init__(self, record):
        self.record = record
        self.records = [record]
        super().__init__("STOP: resource sample " + record["unavailable"])


def proc_record(pid, role, expected_start_ticks=None):
    base = Path("/proc") / str(pid)
    record = dict(pid=pid, role=role, monotonic_ns=time.monotonic_ns(),
                  unix_ns=time.time_ns(), fields={}, memory_status="INCONCLUSIVE")

    def observe(name, action):
        field = dict(available=False, error=None, errno=None, start_ns=time.monotonic_ns(), unix_ns=time.time_ns())
        record["fields"][name] = field
        try:
            value = action()
            field.update(available=True, value=value)
            return value
        except OSError as exc:
            field.update(error=str(exc), errno=exc.errno, filename=exc.filename,
                         error_type=type(exc).__name__)
            return None
        finally:
            field["end_ns"] = time.monotonic_ns()

    def stat_fields(raw):
        return raw[raw.rindex(")") + 2:].split()

    raw_stat = observe("stat", lambda: (base / "stat").read_text())
    status = observe("status", lambda: (base / "status").read_text())
    fields = stat_fields(raw_stat) if raw_stat is not None else None
    parsed = dict(line.split(":", 1) for line in (status or "").splitlines() if ":" in line)
    if fields:
        record.update(raw_stat=raw_stat, start_ticks=int(fields[19]),
                      ppid=int(fields[1]), state=fields[0])
    if status is not None:
        record["raw_status"] = status
    record["identity"] = dict(pid=pid, start_ticks=record.get("start_ticks"),
                              uid=parsed.get("Uid"), gid=parsed.get("Gid"))
    # Retain independent observations before the permission-sensitive FD read.
    for name, key in (("rss_kib", "VmRSS"), ("hwm_kib", "VmHWM"), ("threads", "Threads")):
        value = int(parsed[key].split()[0]) if key in parsed else None
        record[name] = value
        record["fields"][name] = dict(available=value is not None, value=value,
            start_ns=record["fields"]["status"]["start_ns"],
            end_ns=record["fields"]["status"]["end_ns"],
            unix_ns=record["fields"]["status"]["unix_ns"],
            error=None if value is not None else "absent from status", errno=None)

    def ownership():
        st = (base / "fd").stat()
        return dict(uid=st.st_uid, gid=st.st_gid, mode=st.st_mode)

    observe("fd_ownership", ownership)
    record["fds"] = observe("fds", lambda: len(list((base / "fd").iterdir())))
    after = observe("stat_after", lambda: (base / "stat").read_text())
    after_fields = stat_fields(after) if after is not None else None
    same = bool(fields and after_fields and stat_identity(raw_stat)[0] == pid
                and stat_identity(after)[0] == pid and fields[19] == after_fields[19]
                and (expected_start_ticks is None or int(fields[19]) == expected_start_ticks))
    record["identity_verified"] = same
    record["exit_established"] = bool(same and after_fields[0] in ("Z", "X", "x"))
    if after_fields:
        record["state_after"] = after_fields[0]
    errors = [name for name in ("stat", "status", "fds", "stat_after")
              if not record["fields"][name]["available"]]
    if not same:
        # Raw values survive under fields; never expose another identity's metrics.
        for name in ("rss_kib", "hwm_kib", "threads", "fds"):
            record[name] = None
        record["unavailable"] = "identity_changed_or_unverified"
        raise ProcSampleError(record)
    if errors and not (record["exit_established"] and errors == ["fds"]
                       and record["fields"]["fds"]["errno"] in (2, 3, 13)):
        record["unavailable"] = "live_or_ambiguous_field_loss"
        raise ProcSampleError(record)
    if errors or record["exit_established"] or record["rss_kib"] is None or record["hwm_kib"] is None or record["threads"] is None:
        record["unavailable"] = "established_exit" if record["exit_established"] else "memory_missing"
        if not record["exit_established"]:
            raise ProcSampleError(record)
    return record


def stat_identity(raw):
    fields = raw[raw.rindex(")") + 2:].split()
    return int(raw.split(" (", 1)[0]), int(fields[19]), int(fields[1])


def active_invocation(path, runner_pid, context, observed_ns):
    # Runner's start journal is checked against the parent's fixed declaration,
    # never against a receipt's role/selection assertion. Read remains bounded.
    with Path(path).open() as f:
        raw = f.read(65537)
    if len(raw) > 65536:
        raise ValueError("start journal capacity")
    starts = [json.loads(line) for line in raw.splitlines()]
    rows = declaration()["rows"]
    if not starts or len(starts) > len(rows):
        raise ValueError("active invocation missing/capacity")
    previous = context["launch_unix_ns"]
    for index, start in enumerate(starts):
        epoch = start["epoch_unix_ns"]
        if (start["selection"] != rows[index] or start["parent_pid"] != runner_pid
                or start["capture_grant"] != context["capture_grant"]
                or start["runner_generation"] != context["runner_generation"]
                or type(epoch) is not int or not previous <= epoch <= observed_ns):
            raise ValueError("active invocation generation/selection/interval mismatch")
        previous = epoch
    return starts[-1]


def spawn_binding(path, runner_pid, runner_ticks, pid, owner_argv, context, active, observed_ns):
    """One bounded runner projection read; no guessing during publication races."""
    with Path(path).open() as f:
        raw = f.read(16385)
    if len(raw) > 16384:
        raise ValueError("spawn receipt capacity")
    receipt = json.loads(raw)
    parent, ticks, _ = stat_identity(receipt["parent_identity"])
    child, child_ticks, ppid = stat_identity(receipt["child_identity"])
    sel = receipt["selection"]
    if (parent != runner_pid or ticks != runner_ticks or child != pid or ppid != runner_pid
            or receipt["parent_pid"] != runner_pid or receipt["child_pid"] != pid
            or sel != active["selection"] or sel["lane"] not in ("go", "native")
            or receipt["capture_grant"] != context["capture_grant"]
            or receipt["runner_generation"] != context["runner_generation"]
            or receipt["invocation_epoch_unix_ns"] != active["epoch_unix_ns"]
            or receipt["argv"] != owner_argv[sel["lane"]]
            or type(receipt["unix_ns"]) is not int
            or not context["launch_unix_ns"] <= active["epoch_unix_ns"] <= receipt["unix_ns"] <= observed_ns):
        raise ValueError("spawn identity/selection mismatch")
    return dict(start_ticks=child_ticks, role="owner_child", spawn_receipt=receipt)


def resources(runner_pid, identities=None, resolve=None):
    identities = {} if identities is None else identities

    def capacity(kind, limit):
        failed = dict(unavailable="capacity_failure", capacity=kind, limit=limit,
                      monotonic_ns=time.monotonic_ns(), unix_ns=time.time_ns(),
                      memory_status="INCONCLUSIVE")
        failure = ProcSampleError(failed)
        failure.records = records + [failed]
        raise failure

    parent_pid = os.getpid()
    parent = proc_record(parent_pid, "capture_parent", identities.get(parent_pid, {}).get("start_ticks"))
    parent.update(owner_accounted=True, membership=dict(source="supervisor", pid=parent_pid, start_ticks=parent["start_ticks"]))
    records = [parent]
    if len(identities) > 128 or (parent_pid not in identities and len(identities) >= 128):
        capacity("pid_lifetimes", 128)
    identities.setdefault(parent_pid, dict(start_ticks=parent["start_ticks"], role="capture_parent"))
    pending, seen = [runner_pid], set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        if len(seen) >= 32:
            capacity("descendant_processes", 32)
        seen.add(pid)
        try:
            binding = identities.get(pid)
            if pid != runner_pid:
                cause = "no spawn receipt"
                try:
                    binding = resolve(pid) if resolve else None
                except (OSError, ValueError, KeyError, IndexError, TypeError) as exc:
                    binding = None
                    cause = repr(exc)
                if binding is None:
                    # Discovery proves existence, never ownership. Do not sample
                    # or charge an unknown daemon/shim/service as an owner.
                    failed = dict(pid=pid, role="unknown_descendant", unavailable="unbound_descendant",
                        monotonic_ns=time.monotonic_ns(), unix_ns=time.time_ns(),
                        memory_status="INCONCLUSIVE", evidence_error=cause)
                    raise ProcSampleError(failed)
            if pid not in identities and len(identities) >= 128:
                capacity("pid_lifetimes", 128)
            record = proc_record(pid, "runner" if pid == runner_pid else binding["role"],
                                 None if binding is None else binding["start_ticks"])
            if binding is not None and "spawn_receipt" in binding:
                record["spawn_receipt"] = binding["spawn_receipt"]
            record.update(owner_accounted=True, membership=dict(source="Popen" if pid == runner_pid else "runner_spawn",
                          pid=pid, start_ticks=record["start_ticks"]))
            records.append(record)
            identities[pid] = binding or dict(start_ticks=record["start_ticks"], role="runner")
        except ProcSampleError as exc:
            if exc.record.get("unavailable") != "capacity_failure":
                exc.records = records + [exc.record]
            raise
        # Read every thread's children; a Go goroutine may spawn from any TID.
        traversal_field = "task"
        traversal_path = Path("/proc") / str(pid) / "task"
        traversal_start, traversal_unix = time.monotonic_ns(), time.time_ns()
        try:
            tasks = traversal_path.iterdir()
            while True:
                traversal_field = "task"
                traversal_path = Path("/proc") / str(pid) / "task"
                traversal_start, traversal_unix = time.monotonic_ns(), time.time_ns()
                try:
                    task = next(tasks)
                except StopIteration:
                    break
                traversal_field, traversal_path = "children", task / "children"
                traversal_start, traversal_unix = time.monotonic_ns(), time.time_ns()
                pending.extend(map(int, traversal_path.read_text().split()))
        except OSError as exc:
            # ENOENT alone does not establish exit or complete descendant coverage.
            # Keep the sampled identity and batch, without inventing another RSS.
            failed = dict(pid=pid, role=record["role"], start_ticks=record["start_ticks"],
                          identity=record["identity"], identity_verified=record["identity_verified"],
                          monotonic_ns=traversal_start, unix_ns=traversal_unix,
                          memory_status="INCONCLUSIVE", unavailable="traversal_unavailable",
                          fields={traversal_field: dict(available=False, path=str(traversal_path),
                              filename=exc.filename, error=str(exc), error_type=type(exc).__name__,
                              errno=exc.errno, start_ns=traversal_start, unix_ns=traversal_unix,
                              end_ns=time.monotonic_ns())})
            failure = ProcSampleError(failed)
            failure.records = records + [failed]
            raise failure from exc
    return records


def terminate(process, budget):
    receipt = dict(pid=process.pid, requested_ns=time.monotonic_ns())
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    def allowance(cap):
        try:
            return min(cap, budget.remaining(SEAL_SECONDS))
        except RuntimeError:
            receipt["budget_exhausted"] = True
            return 0

    try:
        process.wait(timeout=allowance(.5))
    except subprocess.TimeoutExpired:
        receipt["hard_kill"] = True
    # Parent exit does not prove descendants exited. Kill/check the whole group.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=allowance(.5))
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        receipt.update(group_absent=True, reaped_ns=time.monotonic_ns(), exit=process.returncode)
        return receipt
    raise RuntimeError("STOP: descendant cleanup unconfirmed")


OWNER_LABEL = "inngest.oneshot.owner"


def store_intent(out):
    owner = uuid.uuid4().hex
    identity = dict(schema=1, owner=owner, name="oneshot-" + owner,
                    container_id=None, create_confirmed=False)
    write(Path(out) / "store-owner.json", identity)
    return identity


def store_command(out, budget, argv, cleanup=False):
    """Small, bounded Docker operations; receipt survives errors and timeouts."""
    out = Path(out)
    index = len(list(out.glob("store-command-*.json")))
    if index >= 24:
        raise RuntimeError("STOP: store command capacity")
    receipt = dict(argv=argv, start_ns=time.monotonic_ns(), exit=None,
                   stdout="", stderr="", error=None)
    try:
        timeout = min(2, budget.remaining(SEAL_SECONDS if cleanup else STOP_SECONDS + SEAL_SECONDS))
        if cleanup:
            timeout = min(timeout, budget.store_cleanup_deadline - budget.clock())
            if timeout <= 0:
                raise RuntimeError("STOP: owned store cleanup reserve exhausted")
        result = subprocess.run(argv, env=execution_environment(), capture_output=True,
                                timeout=timeout, check=False)
        receipt.update(exit=result.returncode, stdout=result.stdout[:65536].decode(errors="replace"),
                       stderr=result.stderr[:65536].decode(errors="replace"))
        if len(result.stdout) + len(result.stderr) > 65536:
            raise RuntimeError("STOP: store output capacity")
        return receipt
    except BaseException as exc:
        receipt["error"] = repr(exc)
        if isinstance(exc, subprocess.TimeoutExpired):
            receipt["stdout"] = (exc.stdout or b"")[:65536].decode(errors="replace")
            receipt["stderr"] = (exc.stderr or b"")[:65536].decode(errors="replace")
        raise
    finally:
        receipt["end_ns"] = time.monotonic_ns()
        write(out / ("store-command-%02d.json" % index), receipt)


def prepare_store(out, budget, identity):
    # Creation happens in the supervising parent, before ANY runner exists.
    # Intent/name/label are durable even if create times out before returning ID.
    created = store_command(out, budget, ["docker", "create", "--pull=never", "--name", identity["name"],
        "--label", OWNER_LABEL + "=" + identity["owner"], "-p", "127.0.0.1::6379",
        "valkey/valkey:8.0.3", "valkey-server", "--port", "6379", "--requirepass", "hello", "--appendonly", "yes"])
    container_id = created["stdout"].strip()
    if created["exit"] or not re.fullmatch(r"[0-9a-f]{64}", container_id):
        raise RuntimeError("STOP: store creation failed/unconfirmed")
    identity["container_id"] = container_id
    identity["create_confirmed"] = True
    write(Path(out) / "store-owner.json", identity)
    checked = store_command(out, budget, ["docker", "inspect", "--format",
        '{{json .Id}} {{json .Name}} {{json .Config.Labels}}', identity["container_id"]])
    verify_store_identity(checked, identity, identity["container_id"])
    started = store_command(out, budget, ["docker", "start", identity["container_id"]])
    if started["exit"]:
        raise RuntimeError("STOP: store startup failed")


def verify_store_identity(receipt, identity, container_id):
    # Decode three JSON values without depending on Docker's label key order.
    raw, values, decoder = receipt["stdout"], [], json.JSONDecoder()
    while raw.strip():
        value, end = decoder.raw_decode(raw.lstrip())
        values.append(value)
        raw = raw.lstrip()[end:]
    if (receipt["exit"] or len(values) != 3 or values[0] != container_id
            or values[1] != "/" + identity["name"] or not isinstance(values[2], dict)
            or values[2].get(OWNER_LABEL) != identity["owner"]):
        raise RuntimeError("STOP: store ownership mismatch; removal refused")


class VerifiedStore:
    """Validated parent-owned Docker identity plus authenticated INFO evidence."""
    def __init__(self, owned, inspection, info, before, after):
        data = json.loads(inspection["stdout"])
        pid, ticks, _ = stat_identity(before)
        after_pid, after_ticks, _ = stat_identity(after)
        expected_info = ["docker", "exec", owned["container_id"], "valkey-cli",
                         "--no-auth-warning", "-a", "hello", "INFO", "memory"]
        if (not owned.get("create_confirmed") or not re.fullmatch(r"[0-9a-f]{64}", owned["container_id"])
                or not re.fullmatch(r"[0-9a-f]{32}", owned["owner"])
                or owned["name"] != "oneshot-" + owned["owner"]
                or inspection["exit"] != 0 or inspection.get("error")
                or inspection["argv"] != ["docker", "inspect", "--format", STORE_INSPECT, owned["container_id"]]
                or data["container_id"] != owned["container_id"] or data["name"] != "/" + owned["name"]
                or data["labels"].get(OWNER_LABEL) != owned["owner"] or data["running"] is not True
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", data["image"])
                or pid != data["pid"] or pid <= 0 or (pid, ticks) != (after_pid, after_ticks)
                or " (valkey-server) " not in before or " (valkey-server) " not in after
                or info["argv"] != expected_info or info["exit"] != 0 or info.get("error")
                or not re.search(r"(?m)^used_memory:[0-9]+\r?$", info["stdout"])
                or not re.search(r"(?m)^used_memory_rss:[0-9]+\r?$", info["stdout"])):
            raise ValueError("STOP: required separate-store identity/INFO memory evidence")
        self.pid, self.ticks = pid, ticks
        self.evidence = dict(owned=dict(owned), inspection=inspection, info_memory=info,
                             raw_stat_before=before, raw_stat_after=after,
                             monotonic_ns=time.monotonic_ns(), unix_ns=time.time_ns())


STORE_INSPECT = ('{"container_id":{{json .Id}},"name":{{json .Name}},"labels":{{json .Config.Labels}},'
                 '"pid":{{json .State.Pid}},"running":{{json .State.Running}},"image":{{json .Image}}}')


def bind_store(out, budget, owned):
    evidence = dict(owned=dict(owned), start_ns=time.monotonic_ns(), unix_ns=time.time_ns(), fields={})
    def read_stat(name, pid):
        field = dict(start_ns=time.monotonic_ns(), unix_ns=time.time_ns(), available=False)
        evidence["fields"][name] = field
        try:
            raw = (Path("/proc") / str(pid) / "stat").read_text()
            field.update(available=True, value=raw)
            return raw
        except OSError as exc:
            field.update(error=str(exc), errno=exc.errno, filename=exc.filename)
            raise
        finally:
            field["end_ns"] = time.monotonic_ns()
    try:
        inspected = store_command(out, budget, ["docker", "inspect", "--format", STORE_INSPECT, owned["container_id"]])
        evidence["inspection"] = inspected
        pid = json.loads(inspected["stdout"])["pid"]
        before = read_stat("stat", pid)
        info = store_command(out, budget, ["docker", "exec", owned["container_id"], "valkey-cli",
                                         "--no-auth-warning", "-a", "hello", "INFO", "memory"])
        evidence["info_memory"] = info
        after = read_stat("stat_after", pid)
        bound = VerifiedStore(owned, inspected, info, before, after)
        bound.evidence["fields"] = evidence["fields"]
        evidence["status"] = "VERIFIED"
        return bound
    except BaseException as exc:
        evidence.update(status="STOPPED", error=repr(exc))
        raise
    finally:
        evidence["end_ns"] = time.monotonic_ns()
        write(Path(out) / "store-resource-binding.json", evidence)


def store_resource(binding, projection):
    if (not isinstance(binding, VerifiedStore) or projection["pid"] != binding.pid
            or projection["container_id"] != binding.evidence["owned"]["container_id"]
            or projection["image"] != json.loads(binding.evidence["inspection"]["stdout"])["image"]):
        raise ValueError("STOP: unbound separate-store projection")
    try:
        record = proc_record(binding.pid, "valkey_separate", binding.ticks)
    except ProcSampleError as exc:
        record = exc.record
        # Keep the accepted owner/exit classifier untouched. Only a separately
        # verified owned store may retain optional FD failure while continuing.
        errors = [n for n in ("stat", "status", "fds", "stat_after") if not record["fields"][n]["available"]]
        if (record["unavailable"] != "live_or_ambiguous_field_loss" or errors != ["fds"]
                or not record["identity_verified"] or record["exit_established"]
                or any(record[n] is None for n in ("rss_kib", "hwm_kib", "threads"))):
            raise
        record.pop("unavailable")
        record["diagnostic_unavailable"] = ["fds"]
    if record["exit_established"]:
        record["unavailable"] = "required_store_not_live"
        raise ProcSampleError(record)
    record.update(store_binding=binding.evidence, owner_accounted=False, fds_required=False)
    return record


def cleanup_store(out, budget, identity):
    budget.store_cleanup_deadline = min(budget.start + PACKAGE_SECONDS - SEAL_SECONDS, budget.clock() + 4.5)
    receipt = dict(start_ns=time.monotonic_ns(), status="UNCONFIRMED", identity=dict(identity), error=None)
    try:
        if (not re.fullmatch(r"[0-9a-f]{32}", identity["owner"])
                or identity["name"] != "oneshot-" + identity["owner"]):
            raise RuntimeError("STOP: invalid store ownership intent")
        def call(*args):
            return store_command(out, budget, ["docker", *args], cleanup=True)
        def absent(result, target):
            return (result["exit"] != 0 and not result["stdout"].strip()
                    and result["stderr"].strip() in ("Error: No such object: " + target,
                                                    "Error: No such container: " + target))
        listed = call("ps", "-aq", "--no-trunc", "--filter", "label=" + OWNER_LABEL + "=" + identity["owner"])
        if listed["exit"]:
            raise RuntimeError("STOP: owned store discovery failed")
        ids = listed["stdout"].split()
        if len(ids) > 1 or any(not re.fullmatch(r"[0-9a-f]{64}", i) for i in ids):
            raise RuntimeError("STOP: ambiguous owned store identity")
        known = identity.get("container_id")
        if known and (not re.fullmatch(r"[0-9a-f]{64}", known) or (ids and ids != [known])):
            raise RuntimeError("STOP: owned store ID mismatch")
        target = known or (ids[0] if ids else identity["name"])
        inspected = call("inspect", "--format", '{{json .Id}} {{json .Name}} {{json .Config.Labels}}', target)
        if not absent(inspected, target):
            if not re.fullmatch(r"[0-9a-f]{64}", target):
                raise RuntimeError("STOP: unbound store identity")
            verify_store_identity(inspected, identity, target)
            removed = call("rm", "-f", "-v", target)
            receipt["remove_exit"] = removed["exit"]
            checked = call("inspect", "--format", '{{json .Id}}', target)
            if not absent(checked, target):
                raise RuntimeError("STOP: owned store removal unconfirmed")
        # Check both the unique name and label, including the pre-ID startup window.
        checked_name = call("inspect", "--format", '{{json .Id}}', identity["name"])
        checked_label = call("ps", "-aq", "--no-trunc", "--filter", "label=" + OWNER_LABEL + "=" + identity["owner"])
        if (not absent(checked_name, identity["name"]) or checked_label["exit"] or checked_label["stdout"].strip()):
            raise RuntimeError("STOP: owned store absence unconfirmed")
        if not identity.get("create_confirmed"):
            # A timed-out daemon create may still be in flight. Never infer clean
            # from an empty list/process group; retain unique recovery identity.
            raise RuntimeError("STOP: store create completion unknown; cleanup unconfirmed")
        receipt["status"] = "CONFIRMED_ABSENT"
    except BaseException as exc:
        receipt["error"] = repr(exc)
    finally:
        receipt["end_ns"] = time.monotonic_ns()
        write(Path(out) / "store-cleanup.json", receipt)
    if receipt["status"] != "CONFIRMED_ABSENT":
        raise RuntimeError(receipt["error"])
    return receipt


def source_manifest(root, budget):
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root, env=execution_environment(), timeout=budget.remaining()).split(b"\0")
    names = sorted(set(os.fsdecode(n) for n in names if n))
    if len(names) > 50000:
        raise RuntimeError("STOP: source path capacity")
    result, total = {}, 0
    for name in names:
        budget.remaining()
        p = root / name
        if p.is_file():
            total += p.stat().st_size
            if total > 1024**3:
                raise RuntimeError("STOP: source archive capacity")
            result[name] = sha(p)
    return result


def host():
    paths = ("/proc/loadavg", "/proc/meminfo", "/proc/self/status", "/proc/self/cgroup",
             "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.max",
             "/sys/fs/cgroup/cpuset.cpus.effective")
    return dict(uname=list(os.uname()), python=sys.version, libc=platform.libc_ver(), affinity=sorted(os.sched_getaffinity(0)),
                files={p: Path(p).read_text() if Path(p).exists() else None for p in paths},
                estimator="simultaneous observed parent+runner+owner RSS sum; no subtraction; shared pages may double count",
                limits="5ms is requested only; sparse/missing child lifetimes are INCONCLUSIVE; Valkey is separate; HWM not summed across lifetimes")


def command(argv, root, out, name, budget):
    """Seal every provenance attempt, including spawn/timeout/log failures."""
    log_path, receipt_path = out / (name + ".log"), out / (name + ".command.json")
    if log_path.exists() or receipt_path.exists():
        raise FileExistsError("refuse to overwrite command evidence: " + name)
    env = execution_environment()
    receipt = dict(argv=argv, cwd=str(root), environment=env, start_ns=time.monotonic_ns(),
                   pid=None, process_group=None, identity=None, exit=None,
                   timed_out=False, stop=None, error=None, log_sha256=None)
    process, log, selector, error, failures, retained = None, None, None, None, [], 0
    try:
        log = log_path.open("xb")
        selector = selectors.DefaultSelector()
        timeout = budget.remaining()
        deadline = time.monotonic() + timeout
        process = subprocess.Popen(argv, cwd=root, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, env=env, start_new_session=True)
        budget.process = process
        receipt.update(pid=process.pid, process_group=process.pid,
                       identity=proc_record(process.pid, "provenance_command"))
        selector.register(process.stdout, selectors.EVENT_READ)
        while selector.get_map() or process.poll() is None:
            left = deadline - time.monotonic()
            if left <= 0:
                raise subprocess.TimeoutExpired(argv, timeout)
            for key, _ in selector.select(min(.05, left)):
                block = os.read(key.fd, 8192)
                if not block:
                    selector.unregister(key.fileobj)
                    continue
                keep = block[:max(0, 65536 - retained)]
                if log.write(keep) != len(keep):
                    raise OSError("short command log write")
                retained += len(keep)
                log.flush()
                if len(keep) != len(block):
                    raise RuntimeError("STOP: provenance command log capacity")
        process.wait(timeout=max(.001, deadline - time.monotonic()))
        if process.returncode:
            raise RuntimeError("STOP: provenance command failed: " + name)
    except BaseException as exc:
        error = exc
        receipt["timed_out"] = isinstance(exc, subprocess.TimeoutExpired)
        if isinstance(exc, ProcSampleError):
            receipt["failed_resource_sample"] = exc.record
    finally:
        # Each cleanup/seal action runs even when a preceding action failed.
        # Keep the original command failure as the cause and in the receipt.
        if process is not None:
            try:
                receipt["stop"] = terminate(process, budget)
            except BaseException as exc:
                failures.append("cleanup: " + repr(exc))
            receipt["exit"] = process.returncode
            if budget.process is process and receipt["stop"] and receipt["stop"].get("group_absent"):
                budget.process = None
        actions = []
        if process is not None:
            actions.append(process.stdout.close)
        if selector is not None:
            actions.append(selector.close)
        if log is not None:
            actions.extend((log.flush, lambda: os.fsync(log.fileno()), log.close))
        for action in actions:
            try:
                action()
            except BaseException as exc:
                failures.append("log completion: " + repr(exc))
        try:
            if log_path.exists():
                receipt["log_sha256"] = sha(log_path)
        except BaseException as exc:
            failures.append("log seal: " + repr(exc))
        receipt.update(end_ns=time.monotonic_ns(), retained_log_bytes=retained,
                       status="STOPPED" if error or failures else "COMPLETE",
                       error=(type(error).__name__ + ": " + str(error)) if error else None,
                       sealing_errors=failures)
        try:
            write(receipt_path, receipt)
        except BaseException as exc:
            failures.append("command receipt seal: " + repr(exc))
    if error or failures:
        message = (type(error).__name__ + ": " + str(error)) if error else "STOP: command sealing failed"
        raise RuntimeError(message + ("; " + "; ".join(failures) if failures else "")) from error
    return receipt


def monitor(process, budget, stream, verify, spawn_path=None, owner_argv=None, context=None, starts_path=None):
    identities = {}

    def resolve(pid):
        if spawn_path is None:
            return None
        observed_ns = time.time_ns()
        active = active_invocation(starts_path, process.pid, context, observed_ns)
        return spawn_binding(spawn_path, process.pid, identities[process.pid]["start_ticks"],
                             pid, owner_argv, context, active, observed_ns)

    def sample():
        try:
            records = resources(process.pid, identities, resolve)
        except ProcSampleError as exc:
            stream.emit(dict(kind="resources", records=exc.records, failed=True))
            raise
        stream.emit(dict(kind="resources", records=records))

    while process.poll() is None:
        budget.remaining()
        verify()
        sample()
        time.sleep(min(POLL_SECONDS, budget.remaining()))
    process.wait()
    # poll/wait reaps the runner: its absence is lifecycle evidence, not an RSS zero.
    stream.emit(dict(kind="resources", records=[proc_record(os.getpid(), "capture_parent"),
        dict(pid=process.pid, role="runner", start_ticks=identities.get(process.pid, {}).get("start_ticks"),
             monotonic_ns=time.monotonic_ns(), unix_ns=time.time_ns(),
             unavailable="wait_reaped", exit=process.returncode,
             lifecycle="Popen.poll/wait", memory_status="INCONCLUSIVE")]))
    if process.returncode:
        raise RuntimeError("STOP: functional/owner/validation child failure")
    # Even a clean runner exit cannot hide a surviving helper.
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return
    raise RuntimeError("STOP: runner exited with surviving descendants")


def check_capture_authorization(args):
    grant = validate_authorization(authorization_state(), args.parent_authorization, getattr(args, "out", ""))
    if Path(grant["claim"]).exists() or Path(grant["claim"] + ".runner").exists():
        raise ValueError("capture authorization exhausted")
    return grant


def capture(args):
    budget = Budget()  # includes provenance, stop, container cleanup and sealing
    check_capture_authorization(args)
    # Same alarm/reserve pattern as the reviewed screen, with new one-shot scope.
    def expired(*_):
        if budget.clock() - budget.start >= PACKAGE_SECONDS and budget.process is not None:
            try:
                os.killpg(budget.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        raise RuntimeError("STOP: package alarm; cleanup/sealing unconfirmed if hard budget expired")
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, budget.remaining())
    try:
        return capture_bundle(args, budget)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def capture_bundle(args, budget):
    grant = check_capture_authorization(args)  # before any artifact, provenance or child side effect
    claim_authorization(grant)
    if sys.platform != "linux" or os.uname().machine != "x86_64":
        raise ValueError("native runtime is qualified on Linux amd64 GNU only")
    root, out = Path.cwd(), Path(args.out).resolve()
    if out.is_relative_to(root):
        raise ValueError("artifact directory must be outside the source checkout")
    out.mkdir(parents=True, exist_ok=False)
    process, stream, error, stop = None, None, None, None
    identity, store_cleanup = None, None
    start_ns = time.monotonic_ns()
    manifest = dict(schema=1, status="STARTED", start_ns=start_ns, host=host(),
                    declaration=declaration(), authorization=grant, review_acceptance=REVIEW_ACCEPTANCE,
                    functional_only=False, thresholds_applied=False)
    try:
        sources = source_manifest(root, budget)
        write(out / "sources.json", sources)
        with tarfile.open(out / "sources.tar.gz", "w:gz") as archive:
            for name in sources:
                budget.remaining()
                archive.add(root / name, arcname=name, recursive=False)
        paths = [Path(p).resolve() for p in (args.binary, args.native, args.build_receipt, args.go)]
        original = {str(p): sha(p) for p in paths}
        manifest.update(files=original, source_manifest_sha256=sha(out / "sources.json"),
                        source_archive_sha256=sha(out / "sources.tar.gz"),
                        build_receipt=json.loads(Path(args.build_receipt).read_text()))
        build = manifest["build_receipt"]
        validate_build_contract(build, sources, args.binary, args.native, args.go, root)
        command(["git", "rev-parse", "HEAD"], root, out, "head", budget)
        manifest["base_head"] = (out / "head.log").read_text().strip()
        for name, argv in (("go-version", [args.go, "version"]),
                           ("rust-version", ["rustc", "+1.94.0", "--version"]),
                           ("go-build-info", [args.go, "version", "-m", args.binary])):
            command(argv, root, out, name, budget)
        command(["rustup", "which", "--toolchain", "1.94.0", "rustc"], root, out, "rust-compiler", budget)
        if not (out / "go-version.log").read_text().startswith("go version go1.26.4 linux/amd64\n") or not (out / "rust-version.log").read_text().startswith("rustc 1.94.0 "):
            raise RuntimeError("STOP: effective toolchain version mismatch")
        rust_compiler = Path((out / "rust-compiler.log").read_text().strip())
        original[str(rust_compiler)] = sha(rust_compiler)
        write(out / "selection.json", declaration())
        write(out / "manifest.json", manifest)
        next_check = 0

        def verify(force=False):
            nonlocal next_check
            for name in ("runner.log", "rows.jsonl", "rows.jsonl.starts", "rows.jsonl.readbacks"):
                p = out / name
                if p.exists() and p.stat().st_size > 16 * 1024 * 1024:
                    raise RuntimeError("STOP: row/log artifact capacity")
            store_path = out / "rows.jsonl.store"
            if stream is not None and store_path.exists():
                store = json.loads(store_path.read_text())
                try:
                    record = store_resource(store_binding, store)
                except ProcSampleError as exc:
                    stream.emit(dict(kind="resources", records=exc.records, failed=True))
                    raise
                stream.emit(dict(kind="resources", records=[record]))
            if force or time.monotonic() >= next_check:
                next_check = time.monotonic() + 1
                if (build_configuration(root, execution_environment()) != build["build_configuration"]
                        or source_manifest(root, budget) != sources
                        or any(sha(p) != digest for p, digest in original.items())):
                    raise RuntimeError("STOP: source/binary/script/toolchain provenance changed")

        verify(True)
        validate_authorization(authorization_state(), grant["id"], out)
        identity = store_intent(out)
        prepare_store(out, budget, identity)
        store_binding = bind_store(out, budget, identity)
        env = execution_environment()
        generation = uuid.uuid4().hex
        env.update(RESPONSE_ONESHOT_RUNNER_GENERATION=generation,
                   RESPONSE_ONESHOT_STORE=str(out / "store-owner.json"),
                   RESPONSE_ONESHOT_AUTHORIZATION=grant["id"],
                   RESPONSE_ONESHOT_SELECTION=str(out / "selection.json"),
                   RESPONSE_ONESHOT_ROWS=str(out / "rows.jsonl"),
                   RESPONSE_ONESHOT_SPAWN=str(out / "owner-spawn.json"),
                   RESPONSE_ONESHOT_REMAINING_MS=str(int(budget.remaining() * 1000)),
                   NATIVE_STEP_TEST_BINARY=str(Path(args.native).resolve()),
                   EXPERIMENTAL_KEY_QUEUES_ENABLE="false", GOMAXPROCS="2")
        argv = [str(Path(args.binary).resolve()), "-test.run=^TestResponseOneShotPilot$", "-test.v", "-test.timeout=170s"]
        receipt = dict(argv=argv, cwd=str(root / "pkg/execution/executor"), start_ns=time.monotonic_ns(), launch_unix_ns=time.time_ns(),
                       environment=env)
        write(out / "runner.command.json", receipt)
        stream = Stream(out / "resources.jsonl")
        with (out / "runner.log").open("xb") as log:
            process = subprocess.Popen(argv, cwd=receipt["cwd"], env=env,
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            budget.process = process
            receipt["pid"] = process.pid
            write(out / "runner.command.json", receipt)
            monitor(process, budget, stream, verify, out / "owner-spawn.json",
                    dict(go=[str(Path(args.binary).resolve()), "-test.run=^TestResponseOneShotGoChild$", "-test.timeout=8s"],
                         native=[str(Path(args.native).resolve())]),
                    dict(capture_grant=grant["id"], runner_generation=generation,
                         launch_unix_ns=receipt["launch_unix_ns"]), out / "rows.jsonl.starts")
            log.flush()
            os.fsync(log.fileno())
        receipt.update(exit=process.returncode, end_ns=time.monotonic_ns(), log_sha256=sha(out / "runner.log"))
        write(out / "runner.command.json", receipt)
        verify(True)
        # Full raw validation, never performance gates.
        manifest["reconstruction"] = reconstruct(out / "rows.jsonl", declaration()["rows"], grant)
        manifest["resource_summary"] = resource_summary(out / "resources.jsonl")
    except BaseException as exc:
        error = type(exc).__name__ + ": " + str(exc)
        if isinstance(exc, ProcSampleError):
            manifest["failed_resource_samples"] = exc.records
        # Stop and sealing share the ORIGINAL 180-second hard deadline.
        if signal.getsignal(signal.SIGALRM) != signal.SIG_DFL:
            signal.setitimer(signal.ITIMER_REAL, max(.001, budget.remaining(0)))
        if process is not None:
            try:
                stop = terminate(process, budget)
            except BaseException as cleanup:
                error += "; cleanup: " + repr(cleanup)
    finally:
        if identity is not None:
            try:
                store_cleanup = cleanup_store(out, budget, identity)
            except BaseException as exc:
                error = (error or "") + "; store cleanup: " + repr(exc)
        if process is not None:
            receipt.update(exit=process.returncode, end_ns=time.monotonic_ns(), stop=stop)
            write(out / "runner.command.json", receipt)
        if stream is not None:
            try:
                stream.close()
            except BaseException as exc:
                error = (error or "") + "; artifact write: " + repr(exc)
        manifest.update(status="STOPPED" if error else "COMPLETE_NO_PERFORMANCE_VERDICT",
                        error=error, stop=stop, store_cleanup=store_cleanup, end_ns=time.monotonic_ns())
        # Includes all partial files; refuse to claim a successful seal on errors.
        budget.remaining(0)
        manifest["artifacts"] = {p.name: sha(p) for p in out.iterdir() if p.is_file() and p.name != "manifest.json"}
        write(out / "manifest.json", manifest)
        budget.remaining(0)
    if error:
        raise RuntimeError(error)


def validate_durable(row):
    """Validate Go's independent raw readback, not a Python Redis query."""
    raw = row.get("readback")
    if not isinstance(raw, str) or len(raw.encode()) > 512 * 1024:
        raise ValueError("missing/bounded durable readback")
    if hashlib.sha256(raw.encode()).hexdigest() != row.get("readback_sha256"):
        raise ValueError("durable readback hash mismatch")
    evidence = json.loads(raw)
    if (evidence.get("schema") != "oneshot-durable-v1"
            or any(evidence.get(k) != row[k] for k in
                   ("selection", "child_pid", "child_identity", "epoch_unix_ns"))):
        raise ValueError("durable readback receipt binding mismatch")
    snapshot, sel = evidence["snapshot"], row["selection"]
    run = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    function = "00000000-0000-0000-0000-000000000001"
    account = "00000000-0000-0000-0000-000000000003"
    base = "{estate}:"
    actions, metadata = base + "actions:" + function + ":" + run, base + "metadata:" + run
    saved = sel["scenario"] not in ("reject", "overflow", "savefail")
    keys = {metadata, base + "bulk-events:" + function + ":" + run, base + "key:source-key"}
    if saved or sel["scenario"] == "savefail":
        keys.add(actions)
    if saved:
        keys.add(base + "stack:" + run)
        output = ('{"data":"' + 'x' * (sel["payload"] - 11) + '"}'
                  if sel["payload"] else '{"data":9007199254740993}')
        if sel["scenario"] == "duplicate":
            output = '{"data":"previous"}'
        if snapshot[actions] != dict(Type="hash", Value={"s": output}, PTTL=-1):
            raise ValueError("durable saved output mismatch")
        if snapshot[base + "stack:" + run] != dict(Type="list", Value=["s"], PTTL=-1):
            raise ValueError("durable stack mismatch")
    elif sel["scenario"] == "savefail" and snapshot[actions] != dict(Type="string", Value="wrongtype", PTTL=-1):
        raise ValueError("save failure sentinel mismatch")
    md = snapshot[metadata]
    if (md["Type"] != "hash" or md["Value"]["metadata_size"] != ("17" if saved and sel["scenario"] not in ("idempotent", "duplicate", "zero", "negative") else "6")
            or md["Value"]["step_count"] != ("1" if saved else "0")
            or md["Value"]["state_size"] != str(34 + (len(output.encode()) if saved else 0))):
        raise ValueError("durable accounting mismatch")
    pending = sel["scenario"] == "pending"
    if pending:
        key = base + "pending:" + function + ":" + run
        keys.add(key)
        if snapshot[key] != dict(Type="set", Value=["held"], PTTL=-1):
            raise ValueError("durable pending mismatch")
    queued = saved and not pending and sel["scenario"] != "enqueuefail"
    if queued:
        fixture = json.loads((Path(__file__).resolve().parent.parent /
            "pkg/execution/state/redis_state/testdata/compatibility/native-step-effects-v1.json").read_text())
        expected = fixture["queueSnapshots"]["legacy"]
        queue_keys = {k.replace("QUEUE", "queue") for k in expected}
        keys.update(queue_keys)
        if set(snapshot) != keys:
            raise ValueError("durable exact keys mismatch")
        item_hash = snapshot.get("{queue}:queue:item", {})
        if item_hash.get("Type") != "hash" or set(item_hash.get("Value", {})) != {"3k6yiivdqom1o"}:
            raise ValueError("expected exactly one durable queue item")
        item = json.loads(item_hash["Value"]["3k6yiivdqom1o"])
        want = fixture["queue"]["item"]
        want.update(id="3k6yiivdqom1o", at=row["outcome"]["nowMs"] - 19000,
                    wt=item["wt"], eat=item["eat"])
        want["data"].update(groupID=row["outcome"]["groupId"], pf=19, maxAtts=5,
                            payload={"edge": {"incoming": "source-incoming", "outgoing": "s"}})
        del want["data"]["metadata"]
        if item != want or item["wt"] != item["eat"]:
            raise ValueError("durable queue payload mismatch")
        low = row["outcome"]["nowMs"]
        high = (row["epoch_unix_ns"] + row["times"]["outer_end"]) // 1000000
        if not low <= item["wt"] <= high:
            raise ValueError("durable queue clock bounds")
        for key, value in expected.items():
            key = key.replace("QUEUE", "queue")
            actual = snapshot[key]
            if key == "{queue}:queue:item":
                continue
            if value["Type"] == "zset":
                pointer = key in ("{queue}:partition:sorted", "{queue}:accounts:sorted",
                    "{queue}:accounts:" + account + ":partition:sorted")
                entries = [{"Member": z["Member"].replace("JOB", "3k6yiivdqom1o"),
                            "Score": actual["Value"][0]["Score"] if pointer else 1469922831259}
                           for z in value["Value"]]
                if pointer and not low // 1000 <= entries[0]["Score"] <= high // 1000:
                    raise ValueError("durable pointer bounds")
                value = dict(Type="zset", Value=entries, PTTL=-1)
            if actual != value:
                raise ValueError("durable queue index mismatch")
    elif sel["scenario"] == "enqueuefail":
        keys.add("{queue}:queue:item")
        if snapshot["{queue}:queue:item"] != dict(Type="string", Value="wrongtype", PTTL=-1):
            raise ValueError("enqueue failure sentinel mismatch")
    if set(snapshot) != keys:
        raise ValueError("durable exact keys mismatch")


def reconstruct(path, selection, authorization=None):
    """Reconstruct raw intervals, not thresholds or statistical verdicts."""
    if authorization is not None:
        # Posthoc validation binds the historical admission; expiration after
        # admission does not erase evidence. Live dispatch owns expiry checks.
        state = authorization_state()
        grant = validate_authorization(state, authorization.get("id"), Path(path).parent, now=0)
        if authorization != grant or strict_authorization(Path(grant["claim"]).read_text()) != grant:
            raise ValueError("capture authorization receipt mismatch")
        if Path(grant["claim"] + ".runner").read_text() != grant["id"]:
            raise ValueError("capture runner admission missing")
    if len(selection) > 68:
        raise ValueError("selection capacity")
    invoked, validated, order = {}, {}, []
    with Path(path).open() as f:
        for count, line in enumerate(iter(lambda: f.readline(1024 * 1024 + 1), "")):
            if count >= 136 or len(line) > 1024 * 1024:
                raise ValueError("row retention capacity")
            event = json.loads(line)
            target = invoked if event["kind"] == "invocation" else validated if event["kind"] == "validated" else None
            if target is None:
                raise ValueError("unknown row phase")
            row = event["record"]
            identity = row["selection"]["id"]
            if identity in target:
                raise ValueError("duplicate row identity")
            target[identity] = row
            order.append((event["kind"], identity))
    if set(invoked) != set(validated) or set(invoked) != {s["id"] for s in selection}:
        raise ValueError("partial selection; no replacement or derived verdict")
    if order != [(kind, sel["id"]) for sel in selection for kind in ("invocation", "validated")]:
        raise ValueError("raw dispatch/order mismatch")
    result = []
    for sel in selection:
        row, first = validated[sel["id"]], invoked[sel["id"]]
        if row["selection"] != sel or first["selection"] != sel:
            raise ValueError("declaration/order mismatch")
        if row["error"] or row["exit"] or row.get("blockers") or not row["cleanup"] or not row["validated"]:
            raise ValueError("failed/unclean observation")
        if row["observer_registrations"] or row["observer_deliveries"]:
            raise ValueError("observer construction violated")
        t = row["times"]
        names = ("slot_start", "construction_end", "outer_start", "admitted_projected", "descriptor_encoded", "spawn_start", "spawn_return", "reaped", "decoded", "outer_end", "validation_start", "validation_end", "slot_end")
        values = [t.get(n) for n in names]
        if any(type(v) is not int or v <= 0 for v in values) or any(a > b for a, b in zip(values, values[1:])):
            raise ValueError("invalid boundary")
        # Validation may only append its own boundaries, store diagnostics and
        # blockers. Bind the entire remaining receipt, including future fields.
        mutable = {"times", "validated", "valkey_memory", "blockers", "readback", "readback_sha256"}
        if ({k: v for k, v in row.items() if k not in mutable}
                != {k: v for k, v in first.items() if k not in mutable}):
            raise ValueError("raw invocation receipt changed after validation")
        if first.get("validated") is not False or first.get("blockers") or first.get("readback") or first.get("readback_sha256"):
            raise ValueError("invalid raw invocation validation state")
        validation_times = {"validation_start", "validation_end", "slot_end"}
        if (set(first["times"]) != set(names) - validation_times
                or set(t) != set(names)
                or any(t.get(name) != value for name, value in first["times"].items())):
            raise ValueError("raw invocation boundaries changed after validation")
        expected = {"fresh": "Success", "pending": "Success", "reject": "Error", "savefail": "NeverRetry", "enqueuefail": "AlwaysRetry"}[sel["scenario"]]
        if row["outcome"]["disposition"] != expected:
            raise ValueError("unexpected semantic outcome")
        if sel["scenario"] in ("fresh", "pending"):
            outcome = row["outcome"]
            save, enqueue = outcome.get("save"), outcome.get("enqueue")
            pending = sel["scenario"] == "pending"
            if (not isinstance(save, dict) or save.get("error") != "None"
                    or save.get("hasPending") is not pending
                    or outcome.get("preparationError") is not None
                    or outcome.get("failure") is not None or outcome.get("goError")):
                raise ValueError("declared success profile requires accepted save evidence")
            if pending:
                if enqueue is not None:
                    raise ValueError("pending profile must suppress enqueue")
            elif not isinstance(enqueue, dict) or enqueue.get("class") != "Enqueued":
                raise ValueError("fresh success profile requires Enqueued evidence")
            if sel["lane"] == "native":
                if (save.get("writeStatus") != "Confirmed" or save.get("attempts") != 1
                        or save.get("results") != [[1 if pending else 0]]
                        or save.get("lastFailure") is not None):
                    raise ValueError("native success requires confirmed durable save reply")
                if not pending and (enqueue.get("writeStatus") != "Confirmed"
                        or enqueue.get("result") != 0 or not enqueue.get("storedId")
                        or enqueue.get("lastFailure") is not None):
                    raise ValueError("native fresh success requires confirmed durable enqueue reply")
            elif sel["lane"] != "go":
                raise ValueError("unknown success lane")
        else:
            outcome = row["outcome"]
            if sel["scenario"] in ("reject", "savefail"):
                if outcome.get("enqueue") is not None or outcome.get("groupId") is not None or outcome.get("nowMs") is not None:
                    raise ValueError("pre-enqueue control must suppress discovery/enqueue")
                if sel["scenario"] == "reject" and outcome.get("save") is not None:
                    raise ValueError("rejected profile must suppress save")
                if sel["scenario"] == "savefail" and (not isinstance(outcome.get("save"), dict) or outcome["save"].get("error") in (None, "None")):
                    raise ValueError("save failure requires failed save result")
            elif sel["scenario"] == "enqueuefail":
                if (not isinstance(outcome.get("save"), dict) or outcome["save"].get("error") != "None"
                        or outcome["save"].get("hasPending") is not False
                        or not isinstance(outcome.get("enqueue"), dict) or outcome["enqueue"].get("class") != "Failed"):
                    raise ValueError("enqueue failure requires accepted save and failed enqueue")
        validate_durable(row)
        if row["child_pid"] <= 0 or not row["child_identity"]:
            raise ValueError("child lifetime identity missing")
        result.append(dict(selection=sel, outer_ns=t["outer_end"] - t["outer_start"],
                           construction_ns=t["construction_end"] - t["slot_start"],
                           validation_ns=t["validation_end"] - t["validation_start"],
                           slot_ns=t["slot_end"] - t["slot_start"]))
    return dict(status="RAW_RECONSTRUCTION_ONLY", rows=result,
                review_acceptance=REVIEW_ACCEPTANCE, capture_authorized=authorization is not None,
                # Repeated process starts are never labelled steady state. With
                # five observations there is no p95 gate claim or memory PASS.
                memory="INCONCLUSIVE_PENDING_CHILD_LIFETIME_REVIEW",
                performance_verdict=None, gates_applied=False)


def resource_summary(path):
    """A disclosed estimate only. Raw per-PID HWM is never added across lives."""
    identities, peak_sum, groups = {}, 0, 0
    with Path(path).open() as f:
        for line in iter(lambda: f.readline(1024 * 1024 + 1), ""):
            groups += 1
            if groups > RESOURCE_CAP or len(line) > 1024 * 1024:
                raise ValueError("resource retention capacity")
            event = json.loads(line)
            if event["kind"] != "resources" or len(event["records"]) > 33:
                raise ValueError("resource shape/capacity")
            total = 0
            for r in event["records"]:
                if "unavailable" in r:
                    continue
                key = str(r["pid"]) + ":" + str(r["start_ticks"])
                if key not in identities:
                    if len(identities) >= 128:
                        raise ValueError("PID lifetime capacity")
                    identities[key] = dict(pid=r["pid"], start_ticks=r["start_ticks"],
                                           role=r["role"], first_seen_ns=r["monotonic_ns"],
                                           last_seen_ns=r["monotonic_ns"], count=0,
                                           rss_peak_kib=0, hwm_peak_kib=0, fds_peak=None, threads_peak=0)
                p = identities[key]
                p["last_seen_ns"] = r["monotonic_ns"]
                p["count"] += 1
                for dest, src in (("rss_peak_kib", "rss_kib"), ("hwm_peak_kib", "hwm_kib"),
                                  ("fds_peak", "fds"), ("threads_peak", "threads")):
                    if r[src] is not None:
                        p[dest] = max(p[dest], r[src]) if p[dest] is not None else r[src]
                if r.get("owner_accounted") is True and r["role"] in ("capture_parent", "runner", "owner_child"):
                    total += r["rss_kib"]
            peak_sum = max(peak_sum, total)
    return dict(identities=identities, observed_sum_peak_kib=peak_sum,
                estimator="max sum of co-sampled capture parent/runner/owner RSS; shared pages can double count; no subtraction",
                memory_status="INCONCLUSIVE", limits="sample lifetimes/gaps require independent review; HWM is per identity only; no 5ms scheduling guarantee; no native heap attribution")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("declare")
    red = sub.add_parser("reduce")
    red.add_argument("--rows", required=True)
    cap = sub.add_parser("capture")
    for option in ("out", "binary", "native", "go", "build-receipt", "parent-authorization"):
        cap.add_argument("--" + option, required=True)
    args = parser.parse_args()
    if args.mode == "declare":
        print(json.dumps(declaration(), indent=2))
    elif args.mode == "reduce":
        print(json.dumps(reconstruct(args.rows, declaration()["rows"]), indent=2))
    else:
        capture(args)


if __name__ == "__main__":
    main()
