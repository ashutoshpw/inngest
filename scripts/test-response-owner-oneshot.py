#!/usr/bin/env python3
"""Functional readiness controls and fixed synthetic reconstruction; no benchmark."""
import hashlib
import copy
import importlib.util
import json
import os
import random
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("oneshot", Path(__file__).with_name("response-owner-oneshot.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


SYNTHETIC_AUTHORIZATION = "synthetic-future-grant-01"


class Readiness(unittest.TestCase):
    def future_state(self, args):
        return dict(schema=1, review_acceptance=m.REVIEW_ACCEPTANCE, authorization=dict(
            id=SYNTHETIC_AUTHORIZATION, scope="oneshot-capability-60-plus-8",
            declaration_sha256=m.sha(m.STATE_PATH.with_name("response-oneshot-declaration.json")),
            out=str(Path(args.out).resolve()), claim=str(Path(args.out).with_suffix(".claim").resolve()),
            expires_unix=int(time.time())+600, attempts=1))

    def test_bounded_authorization_contract(self):
        with tempfile.TemporaryDirectory() as td:
            args=type("Args", (), dict(parent_authorization=SYNTHETIC_AUTHORIZATION, out=str(Path(td)/"out")))()
            state=self.future_state(args)
            with mock.patch.object(m,"authorization_state",return_value=state):
                grant=m.check_capture_authorization(args)
                for field,value in (("scope","other"),("expires_unix",0),("attempts",2),
                        ("declaration_sha256","wrong"),("out",td),("id","unrecognized-grant")):
                    bad=copy.deepcopy(state);bad["authorization"][field]=value
                    with self.subTest(field=field),self.assertRaisesRegex(ValueError,"scope mismatch"):
                        m.validate_authorization(bad,args.parent_authorization,args.out)
                    with mock.patch.object(m,"authorization_state",return_value=bad), \
                         mock.patch.object(m,"host",side_effect=AssertionError("host reached")), \
                         mock.patch.object(m.subprocess,"Popen",side_effect=AssertionError("dispatch reached")):
                        with self.assertRaisesRegex(ValueError,"scope mismatch"):
                            m.capture_bundle(args,m.Budget())
                    self.assertEqual([],list(Path(td).iterdir()))
                bad=copy.deepcopy(state);bad["review_acceptance"]="external assertion"
                with self.assertRaisesRegex(ValueError,"unrecognized review"):
                    m.validate_authorization(bad,args.parent_authorization,args.out)
                m.claim_authorization(grant)
                self.assertEqual(grant,m.strict_authorization(Path(grant["claim"]).read_text()))
                with self.assertRaisesRegex(ValueError,"exhausted"):
                    m.check_capture_authorization(args)
                with self.assertRaises(FileExistsError):
                    m.claim_authorization(grant)
                self.assertFalse(Path(args.out).exists())
            with self.assertRaisesRegex(ValueError,"none issued"):
                m.check_capture_authorization(args)
            with self.assertRaisesRegex(ValueError,"duplicate"):
                m.strict_authorization('{"authorization":null,"authorization":null}')

    def test_declaration_scope_and_pairing(self):
        plan = m.declaration()
        rows = plan["rows"]
        self.assertEqual((60, 8, 180), (plan["observations"], plan["controls"], plan["package_seconds"]))
        self.assertEqual((6, 4), (plan["stop_seconds"], plan["seal_seconds"]))
        observations = [r for r in rows if r["phase"] != "control"]
        cells = {}
        for i in range(0, len(observations), 2):
            a, b = observations[i:i + 2]
            self.assertEqual({"go", "native"}, {a["lane"], b["lane"]})
            key = a["seed"], a["payload"], a["observation"]
            self.assertEqual(key, (b["seed"], b["payload"], b["observation"]))
            self.assertNotIn(key, cells)
            cells[key] = (a, b)
        self.assertEqual({(seed, size, obs) for seed in (26011, 26012, 26013)
                          for size in (64, 65536) for obs in range(5)}, set(cells))
        self.assertEqual(["first"] * 12, [r["phase"] for r in rows[:12]])
        self.assertEqual(["repeat_start"] * 48, [r["phase"] for r in rows[12:60]])
        self.assertEqual(8, len({(r["scenario"], r["lane"]) for r in rows[60:]}))
        self.assertEqual(list(range(68)), [r["id"] for r in rows])
        self.assertEqual([("pending", "native"), ("pending", "go"), ("reject", "native"), ("reject", "go"),
                          ("savefail", "native"), ("savefail", "go"), ("enqueuefail", "native"), ("enqueuefail", "go")],
                         [(r["scenario"], r["lane"]) for r in rows[60:]])
        for r in rows[60:]:
            self.assertEqual((26011, 64, 0, "control"), (r["seed"], r["payload"], r["observation"], r["phase"]))
        for r in observations:
            self.assertEqual("fresh", r["scenario"])
            self.assertEqual("first" if r["observation"] == 0 else "repeat_start", r["phase"])
        # Independently reconstruct the pre-existing seeded scheduling contract;
        # the implementation now reads a literal, rather than generating this answer.
        expected = []
        for phase, obs in (("first", (0,)), ("repeat_start", range(1, 5))):
            cells = [(seed, size, n) for seed in (26011, 26012, 26013) for size in (64, 65536) for n in obs]
            random.Random(26027 if phase == "first" else 26028).shuffle(cells)
            for seed, size, n in cells:
                lanes = ["go", "native"]
                random.Random(seed * 100000 + size + n).shuffle(lanes)
                expected.extend((seed, size, n, lane) for lane in lanes)
        self.assertEqual(expected, [(r["seed"], r["payload"], r["observation"], r["lane"]) for r in observations])
        self.assertEqual({"go", "native"}, {rows[i]["lane"] for i in range(0, 60, 2)})

    def test_package_reserves_include_sealing(self):
        now = [1000.0]
        budget = m.Budget(lambda: now[0])
        self.assertEqual(170, budget.remaining())
        now[0] = 1170
        with self.assertRaisesRegex(RuntimeError, "deadline"):
            budget.remaining()
        self.assertEqual(10, budget.remaining(0))
        now[0] = 1180
        with self.assertRaisesRegex(RuntimeError, "deadline"):
            budget.remaining(0)

    def test_raw_reconstruction_fixed_data_and_corruption(self):
        # Independently authored fixed times, NOT samples produced by the helper.
        # Exactly outer=70, construction=9, validation=10, slot=109 ns.
        sel = dict(id=7, seed=26012, payload=64, lane="native", observation=2,
                   phase="repeat_start", scenario="fresh")
        row = dict(selection=sel, epoch_unix_ns=1700000001000000000, parent_pid=101,
                   child_pid=102, child_identity="102 (helper) ... start_ticks=11",
                   argv=["/synthetic/helper"], exit=0, error="", cleanup=True,
                   validated=False, stdout='{"disposition":"Success"}\n', stderr="",
                   observer_registrations=0, observer_deliveries=0,
                   outcome=dict(disposition="Success", nowMs=1700000001000, groupId="00000000-0000-4000-8000-000000000005", save=dict(error="None", hasPending=False,
                       writeStatus="Confirmed", attempts=1, results=[[0]], lastFailure=None),
                       enqueue=None, preparationError=None, failure=None), times=dict(
                       slot_start=1, construction_end=10, outer_start=20,
                       admitted_projected=30, descriptor_encoded=40, spawn_start=50,
                       spawn_return=60, reaped=70, decoded=80, outer_end=90))
        row["outcome"]["enqueue"] = {"class": "Enqueued", "writeStatus": "Confirmed",
                                     "result": 0, "storedId": "3k6yiivdqom1o", "lastFailure": None}
        done = copy.deepcopy(row)
        done["validated"] = True
        done["times"].update(validation_start=95, validation_end=105, slot_end=110)
        events = [dict(kind="invocation", record=row), dict(kind="validated", record=done)]
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "rows.jsonl"

            def attach(values):
                # Literal synthetic store evidence: no Redis/owner is invoked.
                record = values[1]["record"]
                declared = record["selection"]
                run = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                function = "00000000-0000-0000-0000-000000000001"
                base = "{estate}:"
                output = '{"data":"' + 'x' * 53 + '"}'
                snapshot = {
                    base + "metadata:" + run: dict(Type="hash", Value={"metadata_size":"17", "step_count":"1", "state_size":"98"}, PTTL=-1),
                    base + "bulk-events:" + function + ":" + run: dict(Type="string", Value='[{"data":{},"name":"native.test"}]', PTTL=-1),
                    base + "key:source-key": dict(Type="string", Value=run, PTTL=86400000),
                    base + "actions:" + function + ":" + run: dict(Type="hash", Value={"s":output}, PTTL=-1),
                    base + "stack:" + run: dict(Type="list", Value=["s"], PTTL=-1),
                }
                if declared["scenario"] == "pending":
                    snapshot[base + "pending:" + function + ":" + run] = dict(Type="set", Value=["held"], PTTL=-1)
                else:
                    fixture = json.loads((Path(__file__).resolve().parent.parent / "pkg/execution/state/redis_state/testdata/compatibility/native-step-effects-v1.json").read_text())
                    item = fixture["queue"]["item"]
                    item.update(id="3k6yiivdqom1o", at=1699999982000, wt=1700000001000, eat=1700000001000)
                    item["data"].update(groupID="00000000-0000-4000-8000-000000000005", pf=19, maxAtts=5,
                        payload={"edge":{"incoming":"source-incoming", "outgoing":"s"}})
                    del item["data"]["metadata"]
                    for key, value in fixture["queueSnapshots"]["legacy"].items():
                        key = key.replace("QUEUE", "queue")
                        if key == "{queue}:queue:item":
                            value["Value"] = {"3k6yiivdqom1o": json.dumps(item)}
                        elif value["Type"] == "zset":
                            for z in value["Value"]:
                                z["Member"] = z["Member"].replace("JOB", "3k6yiivdqom1o")
                                z["Score"] = 1700000001 if z["Score"] == 1793016000 else 1469922831259
                        snapshot[key] = value
                evidence = {key: record[key] for key in ("selection", "child_pid", "child_identity", "epoch_unix_ns")}
                evidence.update(schema="oneshot-durable-v1", snapshot=snapshot)
                record["readback"] = json.dumps(evidence)
                record["readback_sha256"] = hashlib.sha256(record["readback"].encode()).hexdigest()

            attach(events)
            def check(values):
                path.write_text("".join(json.dumps(v) + "\n" for v in values))
                return m.reconstruct(path, [sel])

            original = copy.deepcopy(events)
            for field, changed in dict(child_identity="different nonempty identity", parent_pid=999,
                    epoch_unix_ns=2000, exit=2, error="failed raw invocation", cleanup=False,
                    argv=["/different/helper"], stderr="retained raw failure", child_usage={"maxrss": 999},
                    observer_deliveries=1).items():
                bad = copy.deepcopy(original)
                bad[0]["record"][field] = changed
                with self.subTest(receipt_field=field), self.assertRaises(ValueError):
                    check(bad)
                self.assertEqual(changed, json.loads(path.read_text().splitlines()[0])["record"][field])
            for lane in ("native", "go"):
                for field, changed in (("enqueue", None), ("enqueue", {"class": "Duplicate"}),
                        ("save", None), ("save", {"error": "None", "hasPending": True})):
                    bad = copy.deepcopy(original)
                    for event in bad:
                        event["record"]["selection"]["lane"] = lane
                        event["record"]["outcome"][field] = changed
                    declared = dict(sel, lane=lane)
                    path.write_text("".join(json.dumps(v) + "\n" for v in bad))
                    with self.subTest(lane=lane, field=field, changed=changed), self.assertRaises(ValueError):
                        m.reconstruct(path, [declared])
            for section, field, changed in (("save", "writeStatus", "MayHaveCommitted"),
                    ("save", "results", []), ("enqueue", "writeStatus", "NotSent"),
                    ("enqueue", "result", None), ("enqueue", "storedId", "")):
                bad = copy.deepcopy(original)
                for event in bad:
                    event["record"]["outcome"][section][field] = changed
                with self.subTest(section=section, field=field), self.assertRaises(ValueError):
                    check(bad)
            # Positive profiles keep their real lane schemas: Go has no native
            # write-status/reply fields, and pending suppresses enqueue entirely.
            for lane in ("native", "go"):
                for scenario in ("fresh", "pending"):
                    good = copy.deepcopy(original)
                    declared = dict(sel, lane=lane, scenario=scenario)
                    for event in good:
                        record = event["record"]
                        record["selection"] = declared
                        record["outcome"]["save"]["hasPending"] = scenario == "pending"
                        record["outcome"]["save"]["results"] = [[1 if scenario == "pending" else 0]]
                        if lane == "go":
                            record["outcome"]["save"] = {"error": "None", "hasPending": scenario == "pending"}
                            record["outcome"]["enqueue"] = {"class": "Enqueued"}
                        if scenario == "pending":
                            record["outcome"]["enqueue"] = None
                    attach(good)
                    path.write_text("".join(json.dumps(v) + "\n" for v in good))
                    with self.subTest(valid_lane=lane, scenario=scenario):
                        self.assertEqual(70, m.reconstruct(path, [declared])["rows"][0]["outer_ns"])
            for mutation in ("missing-item", "missing-index", "pending", "accounting", "output", "identity", "hash", "missing"):
                bad = copy.deepcopy(original)
                record = bad[1]["record"]
                evidence = json.loads(record["readback"])
                snap = evidence["snapshot"]
                if mutation == "missing-item":
                    del snap["{queue}:queue:item"]
                elif mutation == "missing-index":
                    del snap["{queue}:idx:run:01ARZ3NDEKTSV4RRFFQ69G5FAV"]
                elif mutation == "pending":
                    snap["{estate}:pending:unexpected"] = dict(Type="set", Value=["held"], PTTL=-1)
                elif mutation == "accounting":
                    snap["{estate}:metadata:01ARZ3NDEKTSV4RRFFQ69G5FAV"]["Value"]["metadata_size"] = "6"
                elif mutation == "output":
                    snap["{estate}:actions:00000000-0000-0000-0000-000000000001:01ARZ3NDEKTSV4RRFFQ69G5FAV"]["Value"]["s"] = "null"
                elif mutation == "identity":
                    evidence["child_pid"] = 999
                record["readback"] = json.dumps(evidence)
                record["readback_sha256"] = hashlib.sha256(record["readback"].encode()).hexdigest()
                if mutation == "hash":
                    record["readback_sha256"] = "0" * 64
                if mutation == "missing":
                    del record["readback"]
                with self.subTest(durable_corruption=mutation), self.assertRaisesRegex(ValueError, "durable"):
                    check(bad)
            args=type("Args", (), dict(parent_authorization=SYNTHETIC_AUTHORIZATION,out=td))()
            # claim must be outside output; temporary sibling is isolated too.
            state=self.future_state(args)
            with tempfile.TemporaryDirectory() as claims:
                state["authorization"]["claim"]=str(Path(claims)/"claim")
                grant=state["authorization"]
                m.claim_authorization(grant)
                Path(grant["claim"]+".runner").write_text(grant["id"])
                with mock.patch.object(m,"authorization_state",return_value=state):
                    check(original)
                    result=m.reconstruct(path,[sel],grant)
                    self.assertEqual(70,result["rows"][0]["outer_ns"])
                    self.assertTrue(result["capture_authorized"])
                    bad=copy.deepcopy(original)
                    bad[1]["record"]["blockers"]=["genuine effect mismatch"]
                    path.write_text("".join(json.dumps(v)+"\n" for v in bad))
                    with self.assertRaisesRegex(ValueError,"failed/unclean"):
                        m.reconstruct(path,[sel],grant)
                    bad=copy.deepcopy(grant);bad["scope"]="wrong"
                    with self.assertRaisesRegex(ValueError,"receipt mismatch"):
                        m.reconstruct(path,[sel],bad)
            result = check(original)
            self.assertEqual(dict(selection=sel, outer_ns=70, construction_ns=9,
                                  validation_ns=10, slot_ns=109), result["rows"][0])
            self.assertFalse(result["gates_applied"])
            self.assertIsNone(result["performance_verdict"])
            for mutation in ("partial", "duplicate", "boundary", "receipt", "outcome", "cleanup", "observer", "identity", "selection", "blocker", "order"):
                bad = copy.deepcopy(events)
                if mutation == "partial":
                    bad.pop()
                elif mutation == "duplicate":
                    bad.append(copy.deepcopy(bad[0]))
                elif mutation == "boundary":
                    bad[1]["record"]["times"]["outer_end"] = 0
                elif mutation == "receipt":
                    bad[1]["record"]["stdout"] = "rewritten"
                elif mutation == "outcome":
                    for e in bad:
                        e["record"]["outcome"]["disposition"] = "Error"
                elif mutation == "cleanup":
                    bad[1]["record"]["cleanup"] = False
                elif mutation == "observer":
                    bad[1]["record"]["observer_registrations"] = 1
                elif mutation == "identity":
                    bad[1]["record"]["child_identity"] = ""
                elif mutation == "selection":
                    bad[1]["record"]["selection"]["seed"] = 99
                elif mutation == "blocker":
                    bad[1]["record"]["blockers"]=["persisted wall time differs"]
                elif mutation == "order":
                    bad.reverse()
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    check(bad)

    def test_owned_store_cleanup_receipts_and_refusals(self):
        owner, cid = "a" * 32, "b" * 64
        identity = dict(schema=1, owner=owner, name="oneshot-" + owner,
                        container_id=cid, create_confirmed=True)
        def result(stdout="", stderr="", code=0):
            return subprocess.CompletedProcess([], code, stdout.encode(), stderr.encode())
        def missing(target):
            return result(stderr="Error: No such object: " + target + "\n", code=1)
        inspected = result(json.dumps(cid) + " " + json.dumps("/" + identity["name"]) + " " +
                           json.dumps({m.OWNER_LABEL: owner}) + "\n")
        for case in ("removed", "already-absent", "startup-window", "wrong-owner", "wrong-id", "daemon-error", "timeout", "still-present", "reserve"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as td:
                out, budget = Path(td), m.Budget()
                ident = dict(identity)
                calls = [result(cid + "\n"), inspected, result(cid + "\n"), missing(cid),
                         missing(identity["name"]), result()]
                if case == "already-absent":
                    calls = [result(), missing(cid), missing(identity["name"]), result()]
                elif case == "startup-window":
                    ident.update(container_id=None, create_confirmed=False)
                elif case == "wrong-owner":
                    calls[1] = result(json.dumps(cid) + " " + json.dumps("/" + ident["name"]) + " {}")
                elif case == "wrong-id":
                    calls[0] = result("c" * 64 + "\n")
                elif case == "daemon-error":
                    calls = [result(stderr="daemon unavailable", code=1)]
                elif case == "timeout":
                    calls = [subprocess.TimeoutExpired(["docker"], 2)]
                elif case == "still-present":
                    calls[3] = result(json.dumps(cid))
                elif case == "reserve":
                    budget.start -= 177
                with mock.patch.object(m.subprocess, "run", side_effect=calls) as run:
                    if case in ("removed", "already-absent"):
                        self.assertEqual("CONFIRMED_ABSENT", m.cleanup_store(out, budget, ident)["status"])
                    else:
                        with self.assertRaises((RuntimeError, subprocess.TimeoutExpired)):
                            m.cleanup_store(out, budget, ident)
                    for call in run.call_args_list:
                        argv = call.args[0]
                        if "rm" in argv:
                            self.assertEqual(["docker", "rm", "-f", "-v", cid], argv)
                        self.assertLessEqual(call.kwargs["timeout"], 2)
                    if case in ("wrong-owner", "wrong-id", "daemon-error", "timeout", "reserve"):
                        self.assertFalse(any("rm" in call.args[0] for call in run.call_args_list))
                receipt = json.loads((out / "store-cleanup.json").read_text())
                self.assertEqual("CONFIRMED_ABSENT" if case in ("removed", "already-absent") else "UNCONFIRMED", receipt["status"])
                commands = [json.loads(p.read_text()) for p in sorted(out.glob("store-command-*.json"))]
                self.assertTrue(commands)
                self.assertTrue(all(c["end_ns"] >= c["start_ns"] for c in commands))
                if case in ("timeout", "reserve"):
                    self.assertTrue(commands[-1]["error"])

    def test_store_intent_precedes_create_and_id_precedes_start(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            identity = m.store_intent(out)
            cid = "b" * 64
            def docker(argv, **kwargs):
                recorded = json.loads((out / "store-owner.json").read_text())
                self.assertEqual(identity["owner"], recorded["owner"])
                if argv[1] == "create":
                    self.assertIsNone(recorded["container_id"])
                    self.assertIn(m.OWNER_LABEL + "=" + identity["owner"], argv)
                    self.assertIn(identity["name"], argv)
                    output = cid
                else:
                    self.assertEqual(cid, recorded["container_id"])
                    self.assertTrue(recorded["create_confirmed"])
                    output = (json.dumps(cid) + " " + json.dumps("/" + identity["name"]) + " " +
                              json.dumps({m.OWNER_LABEL: identity["owner"]})) if argv[1] == "inspect" else cid
                return subprocess.CompletedProcess(argv, 0, output.encode(), b"")
            with mock.patch.object(m.subprocess, "run", side_effect=docker) as run:
                m.prepare_store(out, m.Budget(), identity)
                self.assertEqual(["create", "inspect", "start"], [c.args[0][1] for c in run.call_args_list])
            self.assertEqual(3, len(list(out.glob("store-command-*.json"))))

    def test_real_child_resources_cleanup_and_partial_failure(self):
        # ONE synthetic sleeping process, not an owner/performance observation.
        with tempfile.TemporaryDirectory() as td:
            stream = m.Stream(Path(td) / "resources.jsonl", cap=4)
            process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                       start_new_session=True)
            try:
                record = m.proc_record(process.pid, "synthetic_child")
                self.assertEqual(process.pid, record["pid"])
                self.assertGreater(record["start_ticks"], 0)
                self.assertGreater(record["rss_kib"], 0)
                self.assertGreaterEqual(record["hwm_kib"], record["rss_kib"])
                self.assertIn("VmRSS:", record["raw_status"])
                stream.emit(dict(kind="started", record=record))

                def drift():
                    raise RuntimeError("STOP: source provenance changed")

                with self.assertRaisesRegex(RuntimeError, "provenance"):
                    m.monitor(process, m.Budget(), stream, drift)
            finally:
                receipt = m.terminate(process, m.Budget())
                stream.emit(dict(kind="stop", receipt=receipt))
                stream.close()
            self.assertTrue(receipt["group_absent"])
            with self.assertRaises(ProcessLookupError):
                os.kill(process.pid, 0)
            retained = [json.loads(x) for x in (Path(td) / "resources.jsonl").read_text().splitlines()]
            self.assertEqual(["started", "stop"], [x["kind"] for x in retained])
            m.write(Path(td) / "failure.json", dict(status="STOPPED", partial_records=2, receipt=receipt))
            self.assertEqual("STOPPED", json.loads((Path(td) / "failure.json").read_text())["status"])

    def test_fixed_resource_estimator_preserves_pid_lifetimes(self):
        def point(pid,ticks,rss,hwm,at,role):
            return dict(pid=pid,start_ticks=ticks,rss_kib=rss,hwm_kib=hwm,
                        monotonic_ns=at,role=role,fds=4,threads=2,owner_accounted=True)
        events=[dict(kind="resources",records=[point(1,10,100,130,1,"capture_parent"),point(2,20,20,40,1,"runner"),point(3,30,5,15,1,"owner_child")]),
                dict(kind="resources",records=[point(1,10,110,130,2,"capture_parent"),point(2,20,25,40,2,"runner"),point(3,31,10,20,2,"owner_child")])]
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/"resources.jsonl"
            path.write_text("".join(json.dumps(e)+"\n" for e in events))
            got=m.resource_summary(path)
        self.assertEqual(145,got["observed_sum_peak_kib"])
        self.assertEqual(4,len(got["identities"]))  # PID 3 was reused
        self.assertEqual(15,got["identities"]["3:30"]["hwm_peak_kib"])
        self.assertEqual(20,got["identities"]["3:31"]["hwm_peak_kib"])
        self.assertEqual("INCONCLUSIVE",got["memory_status"])

    def test_checked_writes_capacity_and_no_capture_authorization(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "stream"
            stream = m.Stream(path, cap=1)
            stream.emit(dict(partial=True))
            with self.assertRaisesRegex(RuntimeError, "capacity"):
                stream.emit(dict(partial=False))
            stream.close()
            self.assertEqual('{"partial":true}\n', path.read_text())
            # A real failing destination leaves the previously sealed record.
            dest = Path(td) / "sealed"
            m.write(dest, dict(status="prior"))
            dest.with_name(dest.name + ".tmp").mkdir()
            with self.assertRaises(OSError):
                m.write(dest, dict(status="replacement"))
            self.assertEqual("prior", json.loads(dest.read_text())["status"])
        class Args:
            parent_authorization = "not authorized"
        with self.assertRaisesRegex(ValueError, "separate parent authorization"):
            m.capture(Args())
        Args.parent_authorization=SYNTHETIC_AUTHORIZATION
        with self.assertRaisesRegex(ValueError,"separate parent authorization"):
            m.capture(Args())

    def test_capture_entry_guards_have_no_side_effects(self):
        # Direct calls must stop before host/source reads, directories or children.
        for entry in (m.capture, lambda args: m.capture_bundle(args, m.Budget())):
            for authorization, message in (("unauthorized", "separate parent authorization"),
                    (SYNTHETIC_AUTHORIZATION, "separate parent authorization")):
                with tempfile.TemporaryDirectory() as td:
                    args = type("Args", (), dict(parent_authorization=authorization, out=str(Path(td)/"out")))()
                    with mock.patch.object(m, "source_manifest", side_effect=AssertionError("source read")), \
                         mock.patch.object(m, "host", side_effect=AssertionError("host read")), \
                         mock.patch.object(m.subprocess, "Popen", side_effect=AssertionError("child spawn")):
                        with self.subTest(entry=entry, authorization=authorization), self.assertRaisesRegex(ValueError, message):
                            entry(args)
                    self.assertEqual([], list(Path(td).iterdir()))

    def test_command_outcomes_seal_bounded_receipts(self):
        # Actual synthetic processes; no repository capture or owner invocation.
        cases = (("ok", [sys.executable, "-c", "print('synthetic ok')"], None),
                 ("failed", [sys.executable, "-c", "import sys; print('raw failure', flush=True); sys.exit(7)"], "provenance command failed"),
                 ("missing", ["/nonexistent/synthetic-helper"], "FileNotFoundError"),
                 ("timeout", [sys.executable, "-c", "import time; print('before timeout', flush=True); time.sleep(60)"], "TimeoutExpired"),
                 ("overflow", [sys.executable, "-c", "import os; os.write(1, b'x'*100000); import time; time.sleep(60)"], "capacity"))
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            started = time.monotonic()
            for name, argv, error in cases:
                # Leave the normal stop/sealing reserves; only command execution expires.
                budget = m.Budget()
                budget.start -= 169.5
                if error:
                    with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, error):
                        m.command(argv, out, out, name, budget)
                else:
                    m.command(argv, out, out, name, budget)
                receipt = json.loads((out/(name+".command.json")).read_text())
                self.assertEqual(argv, receipt["argv"])
                self.assertEqual("STOPPED" if error else "COMPLETE", receipt["status"])
                self.assertEqual(m.sha(out/(name+".log")), receipt["log_sha256"])
                self.assertLessEqual((out/(name+".log")).stat().st_size, 65536)
                self.assertGreaterEqual(receipt["end_ns"], receipt["start_ns"])
                if name != "missing":
                    self.assertEqual(receipt["pid"], receipt["process_group"])
                    self.assertTrue(receipt["stop"]["group_absent"])
                    self.assertIsNotNone(receipt["exit"])
                    with self.assertRaises(ProcessLookupError):
                        os.kill(receipt["pid"], 0)
                if name == "failed":
                    self.assertEqual(7, receipt["exit"])
                    self.assertIn("raw failure", (out/(name+".log")).read_text())
                if name == "timeout":
                    self.assertTrue(receipt["timed_out"])
                    self.assertIn("before timeout", (out/(name+".log")).read_text())
            self.assertLess(time.monotonic()-started, 8)

    def capture_fixture(self, base):
        root = base / "source"
        root.mkdir()
        binary, native, go = [base / n for n in ("executor.test", "helper", "go")]
        for p in (binary, native, go):
            p.write_bytes(p.name.encode())
        env = m.execution_environment()
        commands = {}
        for role, flags in (("go", []), ("go_race", ["-race"])):
            output = binary if role == "go" else base / "race.test"
            commands[role] = dict(argv=[str(go), "test", "-p=2", "-c", *flags,
                "-tags=native_step_integration", "-o", str(output), "./pkg/execution/executor"],
                cwd=str(root), environment=env.copy(), exit=0)
        for role, tail in (("native", ["build", "--release", "-j2", "--bin", "native-step-test-helper"]),
                           ("private_native", ["test", "-j2", "--lib", "--no-run"])):
            commands[role] = dict(argv=["cargo", "+1.94.0", tail[0], "--manifest-path", "rust/Cargo.toml",
                "-p", "native-step-state", "--features", "test-helper", *tail[1:]],
                cwd=str(root), environment=env.copy(), exit=0)
        build = dict(source_code_sha256=m.code_digest({}), go_binary_sha256=m.sha(binary),
            native_binary_sha256=m.sha(native), go_version="go1.26.4", rust_version="1.94.0",
            argv=commands["go"]["argv"].copy(), environment=env.copy(),
            optimization={"go": "default optimized test build; no custom gcflags/ldflags",
                "go_race": "default optimized race test build; no custom gcflags/ldflags",
                "native": "release; repository Cargo profile (panic=unwind)",
                "private_native": "debug test; default Cargo profile"},
            build_configuration=dict(cargo_config="absent", goenv="off", cargo_profile="repository source manifest"),
            build_commands=commands)
        return root, binary, native, go, build

    def test_capture_build_contract_corruptions(self):
        # Reach the actual future-capture gate with real artifact bytes, without
        # authorizing a capture or executing provenance/owner commands.
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            root, binary, native, go, build = self.capture_fixture(base)
            mutations = {"accepted": lambda b: None,
                "optimization": lambda b: b["optimization"].update(native="debug"),
                "receipt-argv": lambda b: b["argv"].insert(2, "-ldflags=-s"),
                "receipt-env": lambda b: b["environment"].update(GOFLAGS="-race"),
                "toolchain": lambda b: b.update(rust_version="1.95.0"),
                "compiler-argv": lambda b: b["build_commands"]["native"]["argv"].__setitem__(1, "+stable"),
                "go-flags": lambda b: b["build_commands"]["go"]["argv"].insert(2, "-gcflags=all=-N"),
                "native-flags": lambda b: b["build_commands"]["native"]["argv"].append("--config=profile.release.opt-level=0"),
                "race-substitution": lambda b: b["build_commands"]["go_race"]["argv"].remove("-race"),
                "environment": lambda b: b["build_commands"]["native"]["environment"].update(RUSTFLAGS="-C opt-level=0"),
                "goenv": lambda b: b["build_commands"]["go"]["environment"].update(GOENV="/tmp/custom"),
                "config": lambda b: b["build_configuration"].update(cargo_config="custom"),
                "missing-command": lambda b: b["build_commands"].pop("private_native"),
                "failed-command": lambda b: b["build_commands"]["go"].update(exit=1)}
            for name, mutate in mutations.items():
                with self.subTest(name=name):
                    candidate = copy.deepcopy(build)
                    mutate(candidate)
                    receipt = base / (name + ".json")
                    receipt.write_text(json.dumps(candidate))
                    args = type("Args", (), dict(parent_authorization=SYNTHETIC_AUTHORIZATION, out=str(base/name),
                        binary=str(binary), native=str(native), go=str(go), build_receipt=str(receipt)))()
                    with mock.patch.object(m, "authorization_state", return_value=self.future_state(args)), mock.patch.object(Path, "cwd", return_value=root), \
                         mock.patch.object(m, "source_manifest", return_value={}), \
                         mock.patch.object(m, "command", side_effect=RuntimeError("accepted gate; no child")) as command:
                        with self.assertRaisesRegex(RuntimeError, "accepted gate; no child" if name == "accepted" else "provenance mismatch"):
                            m.capture_bundle(args, m.Budget())
                        self.assertEqual(1 if name == "accepted" else 0, command.call_count)
            # An untracked ancestor config must also fail before any child.
            (root/".cargo").mkdir()
            (root/".cargo/config.toml").write_text('[profile.release]\nopt-level=0\n')
            with self.assertRaisesRegex(RuntimeError, "Cargo build configuration"):
                m.validate_build_contract(build, {}, binary, native, go, root)

    def test_effective_environment_removes_inherited_controls(self):
        with tempfile.TemporaryDirectory() as td:
            parent = dict(os.environ, GOFLAGS="-race", GODEBUG="asyncpreemptoff=1", GOMAXPROCS="99",
                RUSTFLAGS="-C opt-level=0", CARGO_PROFILE_RELEASE_OPT_LEVEL="0", LD_PRELOAD="/bad.so",
                RESPONSE_ONESHOT_CHILD="poison", API_TOKEN="synthetic-secret", DOCKER_AUTH_CONFIG="synthetic-secret",
                DOCKER_CONFIG=td, TMPDIR=td)
            with mock.patch.dict(os.environ, parent, clear=True):
                m.command([sys.executable, "-c", "import json,os; print(json.dumps(dict(os.environ)))"],
                          Path(td), Path(td), "env", m.Budget())
            actual = json.loads((Path(td)/"env.log").read_text())
            recorded = json.loads((Path(td)/"env.command.json").read_text())["environment"]
            for key in ("RUSTFLAGS", "CARGO_PROFILE_RELEASE_OPT_LEVEL", "LD_PRELOAD", "RESPONSE_ONESHOT_CHILD", "API_TOKEN", "DOCKER_AUTH_CONFIG"):
                self.assertNotIn(key, actual)
                self.assertNotIn(key, recorded)
            self.assertEqual("", actual["GOFLAGS"])
            self.assertEqual("", actual["GODEBUG"])
            self.assertEqual("2", actual["GOMAXPROCS"])
            self.assertEqual(td, actual["DOCKER_CONFIG"])
            self.assertEqual(td, actual["TMPDIR"])
            self.assertTrue(all(actual.get(k) == v for k, v in recorded.items()))
            self.assertNotIn("synthetic-secret", (Path(td)/"env.command.json").read_text())

            # Exercise the actual runner Popen too, using a synthetic executable
            # and mocked provenance commands. No owner, store or pilot runs.
            root, binary, native, go, build = self.capture_fixture(Path(td))
            (root/"pkg/execution/executor").mkdir(parents=True)
            binary.write_text("#!" + sys.executable + "\nimport json,os,sys\nprint(json.dumps(dict(os.environ)),flush=True)\nsys.exit(7)\n")
            binary.chmod(0o755)
            build["go_binary_sha256"] = m.sha(binary)
            build_path = Path(td)/"build.json"
            build_path.write_text(json.dumps(build))
            out = Path(td)/"synthetic-capture"
            args = type("Args", (), dict(parent_authorization=SYNTHETIC_AUTHORIZATION, out=str(out),
                binary=str(binary), native=str(native), go=str(go), build_receipt=str(build_path)))()
            def provenance(argv, root, out, name, budget):
                outputs = {"head": "synthetic", "go-version": "go version go1.26.4 linux/amd64\n",
                    "rust-version": "rustc 1.94.0 (synthetic)\n", "go-build-info": "synthetic",
                    "rust-compiler": str(go)}
                (out/(name+".log")).write_text(outputs[name])
            def synthetic_monitor(process, budget, stream, verify):
                # Resource/cleanup behavior has its own actual-process controls;
                # this case isolates runner environment across the real exec.
                self.assertEqual(7, process.wait(timeout=2))
                raise RuntimeError("synthetic child failure")
            with mock.patch.dict(os.environ, parent, clear=True), mock.patch.object(m, "authorization_state", return_value=self.future_state(args)), \
                 mock.patch.object(Path, "cwd", return_value=root), mock.patch.object(m, "source_manifest", return_value={}), \
                 mock.patch.object(m, "command", side_effect=provenance), mock.patch.object(m, "monitor", side_effect=synthetic_monitor), \
                 mock.patch.object(m, "prepare_store"), mock.patch.object(m, "cleanup_store", return_value={"status": "SYNTHETIC_ENV_ONLY"}) as cleanup:
                with self.assertRaisesRegex(RuntimeError, "child failure"):
                    m.capture_bundle(args, m.Budget())
                cleanup.assert_called_once()
            manifest = json.loads((out/"manifest.json").read_text())
            self.assertEqual("STOPPED", manifest["status"])
            self.assertEqual({"status": "SYNTHETIC_ENV_ONLY"}, manifest["store_cleanup"])
            self.assertTrue(manifest["stop"]["group_absent"])
            self.assertIn("synthetic child failure", manifest["error"])
            actual = json.loads((out/"runner.log").read_text())
            recorded = json.loads((out/"runner.command.json").read_text())["environment"]
            for key in ("RUSTFLAGS", "CARGO_PROFILE_RELEASE_OPT_LEVEL", "LD_PRELOAD", "RESPONSE_ONESHOT_CHILD", "API_TOKEN", "DOCKER_AUTH_CONFIG"):
                self.assertNotIn(key, actual)
                self.assertNotIn(key, recorded)
            self.assertEqual("", actual["GOFLAGS"])
            self.assertEqual("", actual["GODEBUG"])
            self.assertEqual("2", actual["GOMAXPROCS"])
            self.assertEqual(td, actual["DOCKER_CONFIG"])
            self.assertTrue(all(actual.get(k) == v for k, v in recorded.items()))
            self.assertNotIn("synthetic-secret", (out/"runner.command.json").read_text())

    def test_command_sealing_failure_preserves_original_error(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            (out/"failed.command.json.tmp").mkdir()  # actual checked-write failure
            argv = [sys.executable, "-c", "import sys; print('original failure', flush=True); sys.exit(9)"]
            with self.assertRaisesRegex(RuntimeError, "provenance command failed.*command receipt seal") as caught:
                m.command(argv, out, out, "failed", m.Budget())
            self.assertIn("provenance command failed", str(caught.exception.__cause__))
            self.assertEqual("original failure\n", (out/"failed.log").read_text())
            # Existing evidence must never be replaced on another call.
            with self.assertRaises(FileExistsError):
                m.command(argv, out, out, "failed", m.Budget())
            self.assertEqual("original failure\n", (out/"failed.log").read_text())


if __name__ == "__main__":
    unittest.main()
