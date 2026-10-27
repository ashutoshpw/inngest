"""Collector evidence contracts; filesystem fixtures, no child processes."""
import importlib.util
from contextlib import contextmanager, ExitStack
import errno
import json
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("collector", Path(__file__).with_name("response-owner-oneshot.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
EVIDENCE_DIRECTORY = None  # Package runner may retain synthetic final artifacts.


def stat(state="S", ticks=123, pid=77, ppid=10):
    # Linux stat fields 3..22; starttime is field 22, after comm.
    return str(pid) + " (fixture (child)) " + " ".join([state, str(ppid)] + ["0"] * 17 + [str(ticks)]) + "\n"


class Collector(unittest.TestCase):
    def test_unreceipted_descendant_is_fatal(self):
        with tempfile.TemporaryDirectory() as td, self.proc_tree(Path(td), "fds", None):
            with self.assertRaises(m.ProcSampleError) as caught:
                m.resources(101)
            self.assertEqual("unbound_descendant", caught.exception.records[-1]["unavailable"])
            self.assertEqual([100, 101], [r["pid"] for r in caught.exception.records[:-1]])

    def verified_store(self, info_text="used_memory:1024\r\nused_memory_rss:2048\r\n", **changes):
        owned = dict(owner="a" * 32, name="oneshot-" + "a" * 32,
                     container_id="b" * 64, create_confirmed=True)
        data = dict(container_id=owned["container_id"], name="/" + owned["name"],
                    labels={m.OWNER_LABEL: owned["owner"]}, pid=77, running=True, image="sha256:" + "c" * 64)
        data.update(changes)
        inspection = dict(argv=["docker", "inspect", "--format", m.STORE_INSPECT, owned["container_id"]],
                          exit=0, error=None, stdout=json.dumps(data), start_ns=1, end_ns=2)
        info = dict(argv=["docker", "exec", owned["container_id"], "valkey-cli", "--no-auth-warning", "-a", "hello", "INFO", "memory"],
                    exit=0, error=None, stdout=info_text, start_ns=3, end_ns=4)
        raw = stat().replace("fixture (child)", "valkey-server")
        return m.VerifiedStore(owned, inspection, info, raw, raw), data

    def test_verified_store_fd_denial_retains_info_and_null_diagnostic(self):
        binding, projection = self.verified_store()
        record = self.sample(fd_errno=errno.EACCES, binding=binding, projection=projection)
        self.assertEqual(["fds"], record["diagnostic_unavailable"])
        self.assertEqual((47, 63, 2, None), tuple(record[n] for n in ("rss_kib", "hwm_kib", "threads", "fds")))
        self.assertEqual(errno.EACCES, record["fields"]["fds"]["errno"])
        self.assertFalse(record["fields"]["fds"]["available"])
        self.assertFalse(record["owner_accounted"])
        self.assertIn("used_memory:1024", record["store_binding"]["info_memory"]["stdout"])
        self.assertLessEqual(record["fields"]["fds"]["start_ns"], record["fields"]["fds"]["end_ns"])
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "resources.jsonl"
            path.write_text(json.dumps(dict(kind="resources", records=[record])) + "\n")
            summary = m.resource_summary(path)
        self.assertEqual(0, summary["observed_sum_peak_kib"])
        self.assertIsNone(summary["identities"]["77:123"]["fds_peak"])
        self.assertEqual("INCONCLUSIVE", summary["memory_status"])
        # A caller-provided label alone cannot confer the store exception.
        with self.assertRaises(m.ProcSampleError):
            self.sample(fd_errno=errno.EACCES, role="valkey_separate")

    def test_store_required_info_identity_and_other_fields_stay_fatal(self):
        for info in ("", "NOAUTH Authentication required", "used_memory:1024\r\n"):
            with self.subTest(info=info), self.assertRaisesRegex(ValueError, "identity/INFO"):
                self.verified_store(info_text=info)
        for changes in (dict(container_id="d" * 64), dict(labels={}), dict(pid=78), dict(running=False)):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.verified_store(**changes)
        binding, projection = self.verified_store()
        for status in (OSError(errno.EACCES, "status denied", "/proc/77/status"), "VmRSS:\t47 kB\nThreads:\t2\n"):
            with self.subTest(status=str(status)), self.assertRaises(m.ProcSampleError):
                self.sample(fd_errno=errno.EACCES, status=status, binding=binding, projection=projection)
        with self.assertRaises(m.ProcSampleError):
            self.sample(fd_errno=errno.EACCES, after=stat(ticks=456), binding=binding, projection=projection)
        with self.assertRaisesRegex(ValueError, "unbound"):
            self.sample(binding=binding, projection=dict(projection, pid=78))

    def test_spawn_bound_owner_accounting_and_unknown_publication_race(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            receipt_path = root / "spawn.json"
            selection = m.declaration()["rows"][0]
            receipt = dict(parent_pid=101, parent_identity=stat(pid=101, ticks=1010, ppid=100),
                           child_pid=303, child_identity=stat(pid=303, ticks=3030, ppid=101),
                           selection=selection, argv=["/native"], unix_ns=150,
                           capture_grant="fixture-grant", runner_generation="generation-1",
                           invocation_epoch_unix_ns=120)
            context = dict(capture_grant="fixture-grant", runner_generation="generation-1", launch_unix_ns=100)
            active = dict(selection=selection, epoch_unix_ns=120)
            argv = dict(native=["/native"], go=["/go"])
            receipt_path.write_text(json.dumps(receipt))
            with self.proc_tree(root / "proc", "fds", None):
                identities = {}
                records = m.resources(101, identities, lambda pid: m.spawn_binding(receipt_path, 101, 1010, pid, argv, context, active, 200))
                self.assertEqual([100, 101, 303], [r["pid"] for r in records])
                self.assertTrue(all(r["owner_accounted"] for r in records))
                self.assertEqual(receipt, records[-1]["spawn_receipt"])
                stream_path = root / "resources.jsonl"
                stream_path.write_text(json.dumps(dict(kind="resources", records=records)) + "\n")
                summary = m.resource_summary(stream_path)
                self.assertEqual(141, summary["observed_sum_peak_kib"])
                self.assertEqual("INCONCLUSIVE", summary["memory_status"])
                receipt_path.unlink()  # child discovery ahead of publication
                with self.assertRaises(m.ProcSampleError) as caught:
                    m.resources(101, {}, lambda pid: m.spawn_binding(receipt_path, 101, 1010, pid, argv, context, active, 200))
                unknown = caught.exception.records[-1]
                self.assertEqual("unbound_descendant", unknown["unavailable"])
                self.assertEqual("unknown_descendant", unknown["role"])
                self.assertNotIn("rss_kib", unknown)
                self.assertNotIn("owner_accounted", unknown)
            # Foreign runner, PID reuse, foreign PPID, wrong child and wrong
            # executable cannot borrow a reviewed lane label.
            for change in (dict(parent_identity=stat(pid=101, ticks=999)),
                           dict(child_identity=stat(pid=303, ticks=3030, ppid=9)),
                           dict(child_pid=404), dict(argv=["/docker-shim"]),
                           dict(capture_grant="old-grant"), dict(runner_generation="old-generation"),
                           dict(invocation_epoch_unix_ns=119), dict(unix_ns=99), dict(unix_ns=201)) :
                receipt_path.write_text(json.dumps(dict(receipt, **change)))
                with self.subTest(change=change), self.assertRaises(ValueError):
                    m.spawn_binding(receipt_path, 101, 1010, 303, argv, context, active, 200)

    def test_cached_identity_requires_current_receipt(self):
        with tempfile.TemporaryDirectory() as td, self.proc_tree(Path(td), "fds", None):
            identities = {303: dict(start_ticks=3030, role="owner_child")}
            with self.assertRaises(m.ProcSampleError) as caught:
                m.resources(101, identities, lambda pid: None)
            self.assertEqual("unbound_descendant", caught.exception.record["unavailable"])
            self.assertNotIn("rss_kib", caught.exception.record)

    def test_current_generation_and_invocation_transitions(self):
        with tempfile.TemporaryDirectory() as td, self.proc_tree(Path(td) / "proc", "fds", None):
            path, starts = Path(td) / "spawn", Path(td) / "starts"
            context = dict(capture_grant="fixture-grant", runner_generation="new-generation", launch_unix_ns=100)
            rows = m.declaration()["rows"]
            journal = []
            identities = {}
            for index in range(2):
                epoch = 120 + index * 20
                journal.append(dict(context, parent_pid=101, selection=rows[index], epoch_unix_ns=epoch))
                starts.write_text("".join(json.dumps(r) + "\n" for r in journal))
                current = dict(capture_grant=context["capture_grant"], runner_generation=context["runner_generation"],
                    parent_pid=101, parent_identity=stat(pid=101, ticks=1010, ppid=100),
                    child_pid=303, child_identity=stat(pid=303, ticks=3030, ppid=101),
                    selection=rows[index], argv=["/"+rows[index]["lane"]],
                    invocation_epoch_unix_ns=epoch, unix_ns=epoch+1)
                argv = dict(go=["/go"], native=["/native"])
                def resolve(pid):
                    active = m.active_invocation(starts, 101, context, 200)
                    return m.spawn_binding(path, 101, 1010, pid, argv, context, active, 200)
                stale = dict(current, runner_generation="old-generation") if index == 0 else previous
                path.write_text(json.dumps(stale))
                with self.subTest(index=index, receipt="stale"):
                    with self.assertRaises(m.ProcSampleError) as caught:
                        m.resources(101, identities, resolve)
                    self.assertEqual("unbound_descendant", caught.exception.record["unavailable"])
                path.write_text(json.dumps(current))
                records = m.resources(101, identities, resolve)
                self.assertEqual(current, records[-1]["spawn_receipt"])
                previous = current
            # A self-consistent caller selection/role cannot alter the parent's grid.
            journal[-1]["selection"] = rows[3]
            starts.write_text("".join(json.dumps(r) + "\n" for r in journal))
            with self.assertRaisesRegex(ValueError, "selection"):
                m.active_invocation(starts, 101, context, 200)

    def sample(self, fd_errno=None, after=None, status=None, expected=None, role="owner_child", binding=None, projection=None):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        root = Path(td.name)
        proc = root / "77"
        proc.mkdir()
        (proc / "fd").mkdir()
        raw_status = status if status is not None else "State:\tS (sleeping)\nUid:\t1002 1002 1002 1002\nGid:\t1002 1002 1002 1002\nVmRSS:\t47 kB\nVmHWM:\t63 kB\nThreads:\t2\n"
        original_read, original_iter = Path.read_text, Path.iterdir
        reads = iter([stat(), stat() if after is None else after])

        def read(path, *args, **kwargs):
            if path == proc / "stat":
                result = next(reads)
                if isinstance(result, BaseException):
                    raise result
                return result
            if path == proc / "status":
                if isinstance(raw_status, OSError):
                    raise raw_status
                return raw_status
            return original_read(path, *args, **kwargs)

        def entries(path):
            if path == proc / "fd" and fd_errno:
                raise OSError(fd_errno, "fixture FD failure", str(path))
            return original_iter(path)

        with mock.patch.object(m, "Path", return_value=root), \
             mock.patch.object(Path, "read_text", read), \
             mock.patch.object(Path, "iterdir", entries):
            return m.store_resource(binding, projection) if binding is not None else m.proc_record(77, role, expected)

    def test_partial_fields_survive_live_and_ambiguous_fd_loss(self):
        for number in (errno.EACCES, errno.ENOENT, errno.ESRCH):
            try:
                self.sample(fd_errno=number)
            except m.ProcSampleError as exc:
                r = exc.record
            else:
                self.fail("live/ambiguous FD loss did not fail-stop")
            self.assertEqual((47, 63, 2, None), (r["rss_kib"], r["hwm_kib"], r["threads"], r["fds"]))
            self.assertEqual(123, r["start_ticks"])
            self.assertIn("Uid:", r["raw_status"])
            self.assertIn("fixture (child)", r["raw_stat"])
            self.assertEqual(number, r["fields"]["fds"]["errno"])
            self.assertFalse(r["exit_established"])
            self.assertLessEqual(r["fields"]["rss_kib"]["end_ns"], r["fields"]["fds"]["start_ns"])
            self.assertTrue(r["fields"]["fd_ownership"]["available"])
            self.assertEqual("INCONCLUSIVE", r["memory_status"])

    def test_only_confirmed_same_identity_exit_can_tolerate_fd_loss(self):
        for number in (None, errno.EACCES, errno.ENOENT, errno.ESRCH):
            with self.subTest(errno=number):
                r = self.sample(fd_errno=number, after=stat("Z"),
                    status="State:\tZ (zombie)\nUid:\t1002\nGid:\t1002\nThreads:\t1\n")
                self.assertTrue(r["identity_verified"] and r["exit_established"])
                self.assertEqual("established_exit", r["unavailable"])
                self.assertIsNone(r["rss_kib"])
                self.assertIsNone(r["hwm_kib"])
                self.assertEqual("INCONCLUSIVE", r["memory_status"])
        with self.assertRaises(m.ProcSampleError):
            self.sample(fd_errno=errno.EIO, after=stat("Z"))

    def test_pid_reuse_or_unverified_exit_never_exposes_metrics(self):
        for after, expected in ((stat("Z", 456), None), (stat(pid=88), None), (FileNotFoundError(errno.ENOENT, "gone"), None), (stat(), 456)):
            with self.subTest(after=repr(after), expected=expected), self.assertRaises(m.ProcSampleError) as caught:
                self.sample(fd_errno=errno.EACCES, after=after, expected=expected)
            r = caught.exception.record
            self.assertFalse(r["identity_verified"])
            self.assertFalse(r["exit_established"])
            self.assertIsNone(r["rss_kib"])
            self.assertEqual(47, r["fields"]["rss_kib"]["value"])
            self.assertEqual("identity_changed_or_unverified", r["unavailable"])

    def test_status_oserror_is_fatal_even_with_confirmed_exit(self):
        filename = "/proc/77/status"
        with self.assertRaises(m.ProcSampleError) as caught:
            self.sample(after=stat("Z"), status=OSError(errno.EACCES, "fixture status denial", filename))
        record = caught.exception.record
        self.assertTrue(record["exit_established"])
        self.assertEqual("live_or_ambiguous_field_loss", record["unavailable"])
        self.assertIsNone(record["rss_kib"])
        field = record["fields"]["status"]
        self.assertEqual(errno.EACCES, field["errno"])
        self.assertEqual(filename, field["filename"])
        self.assertIn(filename, field["error"])

    @contextmanager
    def proc_tree(self, root, field, number):
        # Controlled kernel files/operations, never fabricated collector records.
        parent, runner, later = 100, 101, 303
        descendants = [later] + (list(range(304, 335)) if field == "descendant_capacity" else [])
        for pid in [parent, runner] + descendants:
            base = root / str(pid)
            (base / "fd").mkdir(parents=True)
            (base / "stat").write_text(stat(ticks=pid * 10, pid=pid, ppid=runner if pid in descendants else parent))
            (base / "status").write_text("Uid:\t1002\nGid:\t1002\nVmRSS:\t47 kB\nVmHWM:\t63 kB\nThreads:\t2\n")
            task = base / "task" / str(pid)
            task.mkdir(parents=True)
            (task / "children").write_text(" ".join(map(str, reversed(descendants))) if pid == runner else "")
        relative = {"fds": "fd", "task": "task", "children": "task/303/children", "unknown": "fd", "identity_capacity": "fd", "descendant_capacity": "fd"}[field]
        failed_path = root / str(later) / relative
        # A filename supplied by the OS may differ from the attempted path.
        filename = str(failed_path / "kernel-detail")
        original_read, original_iter = Path.read_text, Path.iterdir

        def read(path, *args, **kwargs):
            if number is not None and field == "children" and path == failed_path:
                raise OSError(number, "fixture traversal denial", filename)
            return original_read(path, *args, **kwargs)

        def entries(path):
            if number is not None and field in ("task", "fds") and path == failed_path:
                # Fail during iteration too, rather than only at iterator creation.
                yield from ()
                raise OSError(number, "fixture proc denial", filename)
            yield from original_iter(path)

        def path(value):
            return root if value == "/proc" else Path(value)

        with mock.patch.object(m, "Path", side_effect=path, wraps=Path), \
             mock.patch.object(m.os, "getpid", return_value=parent), \
             mock.patch.object(Path, "read_text", read), \
             mock.patch.object(Path, "iterdir", entries):
            yield [parent, runner, later], failed_path, filename

    def test_resources_keeps_batch_and_prior_pid_identity_on_mismatch(self):
        with tempfile.TemporaryDirectory() as td, self.proc_tree(Path(td), "fds", errno.EACCES) as fixture:
            pids, _, _ = fixture
            identities = {pids[-1]: dict(start_ticks=999, role="owner_child")}
            with self.assertRaises(m.ProcSampleError) as caught:
                m.resources(pids[1], identities, lambda pid: identities[pid])
            records = caught.exception.records
            self.assertEqual(pids, [r["pid"] for r in records])
            self.assertEqual({100: 1000, 101: 1010, 303: 999}, {p: b["start_ticks"] for p, b in identities.items()})
            failed = records[-1]
            self.assertEqual("identity_changed_or_unverified", failed["unavailable"])
            self.assertFalse(failed["identity_verified"])
            self.assertIsNone(failed["rss_kib"])
            self.assertEqual(47, failed["fields"]["rss_kib"]["value"])

    def test_resource_batch_reaches_monitor_and_final_capture_manifest(self):
        cases = [("unknown", None), ("identity_capacity", None), ("descendant_capacity", None), ("fds", errno.EACCES)] + [(field, number)
            for field in ("task", "children") for number in (errno.EACCES, errno.EIO, errno.ENOENT)]
        for field, number in cases:
            with self.subTest(field=field, errno=number), tempfile.TemporaryDirectory() as td, ExitStack() as stack:
                base = Path(td)
                proc = base / "proc"
                pids, failed_path, filename = stack.enter_context(self.proc_tree(proc, field, number))
                out = base / "capture"
                binary, native, go, build = [base / name for name in ("binary", "native", "go", "build.json")]
                for path in (binary, native, go):
                    path.write_text("synthetic bytes; never executed")
                build.write_text(json.dumps({"build_configuration": {}}))
                args = SimpleNamespace(out=str(out), binary=str(binary), native=str(native),
                    go=str(go), build_receipt=str(build), parent_authorization="fixture-only")
                process = SimpleNamespace(pid=pids[1], returncode=None, poll=lambda: None)

                def provenance(argv, root, output, name, budget):
                    logs = {"head": "synthetic", "go-version": "go version go1.26.4 linux/amd64\n",
                        "rust-version": "rustc 1.94.0 (synthetic)\n", "go-build-info": "synthetic",
                        "rust-compiler": str(go)}
                    (output / (name + ".log")).write_text(logs[name])

                # Protected test context only: public authorization is unchanged.
                patches = {
                    "check_capture_authorization": {"return_value": {"id": "fixture-only"}},
                    "claim_authorization": {}, "validate_authorization": {},
                    "authorization_state": {"return_value": {}},
                    "host": {"return_value": {"synthetic": True}},
                    "source_manifest": {"return_value": {}},
                    "validate_build_contract": {}, "build_configuration": {"return_value": {}},
                    "command": {"side_effect": provenance},
                    "store_intent": {"return_value": None}, "prepare_store": {}, "bind_store": {"return_value": None},
                    "cleanup_store": {"side_effect": AssertionError("no store exists")},
                    "terminate": {"return_value": {"synthetic": True}},
                }
                for name, options in patches.items():
                    stack.enter_context(mock.patch.object(m, name, **options))
                def launch(*args, **kwargs):
                    env = kwargs["env"]
                    now = m.time.time_ns()
                    context = dict(capture_grant=env["RESPONSE_ONESHOT_AUTHORIZATION"],
                                   runner_generation=env["RESPONSE_ONESHOT_RUNNER_GENERATION"])
                    starts = dict(context, selection=m.declaration()["rows"][0], epoch_unix_ns=now, parent_pid=101)
                    (out / "rows.jsonl.starts").write_text(json.dumps(starts) + "\n")
                    if field != "unknown":
                        projection = dict(context, parent_pid=101, parent_identity=stat(ticks=1010, pid=101, ppid=100),
                            child_pid=303, child_identity=stat(ticks=3030, pid=303, ppid=101),
                            selection=starts["selection"], argv=[str(native)], unix_ns=now,
                            invocation_epoch_unix_ns=now)
                        (out / "owner-spawn.json").write_text(json.dumps(projection))
                    return process

                if field in ("identity_capacity", "descendant_capacity"):
                    original_resources = m.resources
                    def capacity_resources(runner, identities, resolve):
                        if field == "identity_capacity":
                            identities.update({pid: dict(start_ticks=pid*10, role="runner")
                                               for pid in [100, 101] + list(range(1000, 1126))})
                        # Capacity fixture admits bounded, actual proc samples. Membership
                        # is independently exercised in the generation/transition cases.
                        try:
                            return original_resources(runner, identities,
                                lambda pid: dict(start_ticks=pid*10, role="owner_child"))
                        finally:
                            self.assertLessEqual(len(identities), 128)
                    stack.enter_context(mock.patch.object(m, "resources", side_effect=capacity_resources))
                popen = stack.enter_context(mock.patch.object(m.subprocess, "Popen", side_effect=launch))
                stack.enter_context(mock.patch.object(m.subprocess, "run", side_effect=AssertionError("no commands")))
                stack.enter_context(mock.patch.object(m.os, "killpg", side_effect=AssertionError("no process group")))
                stack.enter_context(mock.patch.object(m.time, "sleep", side_effect=AssertionError("continued after failure")))
                stack.enter_context(mock.patch.object(m.signal, "getsignal", return_value=m.signal.SIG_DFL))
                with self.assertRaisesRegex(RuntimeError, "ProcSampleError"):
                    m.capture_bundle(args, m.Budget())
                popen.assert_called_once()
                manifest = json.loads((out / "manifest.json").read_text())
                if EVIDENCE_DIRECTORY is not None:
                    shutil.copytree(out, EVIDENCE_DIRECTORY / (field + "-" + str(number)))
                self.assertEqual("STOPPED", manifest["status"])
                event = json.loads((out / "resources.jsonl").read_text())
                self.assertTrue(event["failed"])
                records = event["records"]
                self.assertEqual(records, manifest["failed_resource_samples"])
                if field in ("identity_capacity", "descendant_capacity"):
                    failed = records[-1]
                    self.assertEqual("capacity_failure", failed["unavailable"])
                    self.assertEqual("pid_lifetimes" if field == "identity_capacity" else "descendant_processes", failed["capacity"])
                    self.assertEqual(128 if field == "identity_capacity" else 32, failed["limit"])
                    self.assertEqual(2 if field == "identity_capacity" else 33, len(records)-1)
                    if field == "descendant_capacity":
                        self.assertNotIn(334, [r["pid"] for r in records[:-1]])
                    self.assertNotIn("pid", failed)
                    self.assertNotIn("rss_kib", failed)
                    self.assertTrue(all(r["owner_accounted"] and r["rss_kib"] == 47 for r in records[:-1]))
                    for name, digest in manifest["artifacts"].items():
                        self.assertEqual(digest, m.sha(out / name))
                    continue
                self.assertEqual(pids if field in ("fds", "unknown") else pids + [pids[-1]], [r["pid"] for r in records])
                if field == "unknown":
                    self.assertEqual("unbound_descendant", records[-1]["unavailable"])
                    self.assertNotIn("rss_kib", records[-1])
                    for name, digest in manifest["artifacts"].items():
                        self.assertEqual(digest, m.sha(out / name))
                    continue
                self.assertEqual(["capture_parent", "runner", "owner_child"], [r["role"] for r in records[:3]])
                self.assertEqual([47, 47, 47], [r["rss_kib"] for r in records[:3]])
                failed = records[-1]
                self.assertEqual(3030, failed["identity"]["start_ticks"])
                self.assertTrue(failed["identity_verified"])
                observation = failed["fields"][field]
                self.assertFalse(observation["available"])
                self.assertEqual(number, observation["errno"])
                self.assertEqual(filename, observation["filename"])
                self.assertIn(filename, observation["error"])
                if field != "fds":
                    self.assertEqual(str(failed_path), observation["path"])
                    self.assertEqual("traversal_unavailable", failed["unavailable"])
                self.assertGreater(observation["unix_ns"], 0)
                self.assertLessEqual(observation["start_ns"], observation["end_ns"])
                self.assertEqual("INCONCLUSIVE", failed["memory_status"])
                # FINAL artifact bytes and hashes, through the real sealer.
                self.assertIn("resources.jsonl", manifest["artifacts"])
                for name, digest in manifest["artifacts"].items():
                    self.assertEqual(digest, m.sha(out / name))


if __name__ == "__main__":
    unittest.main()
