#![cfg(feature = "test-helper")]
use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn helper_rejects_oversize_instead_of_accepting_a_valid_prefix() {
    let input = br#"{"store":{"address":"127.0.0.1:1","password":"unused","connectTimeoutMs":50,"readTimeoutMs":50,"writeTimeoutMs":50},"state":{"prefix":"estate","layout":"NeverShard","identity":{"runId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","functionId":"00000000-0000-0000-0000-000000000001","accountId":"00000000-0000-0000-0000-000000000003","envId":"00000000-0000-0000-0000-000000000002","appId":"00000000-0000-0000-0000-000000000004"}},"budgetMs":100,"disabledRetries":true,"operation":{"kind":"prepare","response":[91,123,34,111,112,34,58,34,83,116,101,112,82,117,110,34,44,34,105,100,34,58,34,115,34,125,93],"config":{"requestVersion":2,"forceStepPlan":false,"traceDisabled":true,"hasAi":false,"sdkMetadata":[],"experiments":[],"loadedStateBytes":0,"stateLimit":null}}}"#;
    for extra in [0, 1] {
        let mut raw = input.to_vec();
        raw.resize(32 * 1024 * 1024 + extra, b' ');
        let mut child = Command::new(env!("CARGO_BIN_EXE_native-step-test-helper"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&raw).unwrap();
        let out = child.wait_with_output().unwrap();
        if extra == 0 {
            assert!(out.status.success());
            let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
            assert_eq!(value["step"], "s");
        } else {
            assert_eq!(
                out.status.code(),
                Some(2),
                "oversized valid prefix must fail closed"
            );
            assert!(out.stdout.is_empty());
            assert_eq!(out.stderr, b"helper input exceeds 32 MiB\n");
        }
    }
}

#[test]
fn standalone_enqueue_preserves_validation_precedence_without_io() {
    use serde_json::json;
    use std::net::TcpListener;

    // A live listener detects even a connection/handshake before rejection.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = json!({
        "store": {"address": listener.local_addr().unwrap().to_string(), "password": "unused",
            "connectTimeoutMs": 50, "readTimeoutMs": 50, "writeTimeoutMs": 50},
        "state": {"prefix": "state", "layout": "NeverShard", "identity": {
            "runId": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "functionId": "00000000-0000-0000-0000-000000000001",
            "accountId": "00000000-0000-0000-0000-000000000003",
            "envId": "00000000-0000-0000-0000-000000000002",
            "appId": "00000000-0000-0000-0000-000000000004"}},
        "budgetMs": 100, "disabledRetries": true,
        "operation": {"kind": "enqueue", "response": br#"[{"op":"StepRun","id":"s"}]"#.to_vec(),
            "config": {"requestVersion": 2, "forceStepPlan": false, "traceDisabled": true,
                "hasAi": false, "sdkMetadata": [], "experiments": [],
                "loadedStateBytes": 34, "stateLimit": 0},
            "discovery": {"queue": {"prefix": "queue", "mode": "Partition"},
                "saveCompleted": true, "hasPending": false, "idempotencyKey": "run-key",
                "incoming": "function", "groupId": "", "carrier": null, "nowMs": 1,
                "priorityFactor": 7, "identifierPriorityFactor": null, "maxAttempts": 3,
                "functionVersion": 0, "eventId": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
                "eventIds": ["01ARZ3NDEKTSV4RRFFQ69G5FAW"]}}
    });
    for (name, now, pending, saved, events, state_prefix, queue_prefix, expected) in [
        (
            "pending-future",
            1,
            true,
            true,
            1,
            "state",
            "queue",
            "Ineligible",
        ),
        (
            "pending-valid-clock",
            1_700_000_000_000_i64,
            true,
            true,
            1,
            "state",
            "queue",
            "Ineligible",
        ),
        (
            "unsaved-future",
            1,
            false,
            false,
            1,
            "state",
            "queue",
            "Ineligible",
        ),
        (
            "multiple-events-future",
            1,
            false,
            true,
            2,
            "state",
            "queue",
            "Ineligible",
        ),
        (
            "pending-negative-clock",
            -1,
            true,
            true,
            1,
            "state",
            "queue",
            "Ineligible",
        ),
        (
            "pending-zero-clock",
            0,
            true,
            true,
            1,
            "state",
            "queue",
            "Ineligible",
        ),
        (
            "pending-invalid-queue",
            1,
            true,
            true,
            1,
            "state",
            "",
            "Ineligible",
        ),
        (
            "pending-invalid-state",
            1,
            true,
            true,
            1,
            "",
            "queue",
            "InvalidConfiguration",
        ),
        (
            "eligible-future",
            1,
            false,
            true,
            1,
            "state",
            "queue",
            "InvalidConfiguration",
        ),
    ] {
        let mut input = base.clone();
        input["state"]["prefix"] = json!(state_prefix);
        let d = &mut input["operation"]["discovery"];
        d["nowMs"] = json!(now);
        d["hasPending"] = json!(pending);
        d["saveCompleted"] = json!(saved);
        d["queue"]["prefix"] = json!(queue_prefix);
        if events == 2 {
            d["eventIds"] = json!(["01ARZ3NDEKTSV4RRFFQ69G5FAW", "01ARZ3NDEKTSV4RRFFQ69G5FAX"]);
        }
        let mut child = Command::new(env!("CARGO_BIN_EXE_native-step-test-helper"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&serde_json::to_vec(&input).unwrap())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success(), "{name}: {:?}", out.stderr);
        assert!(out.stderr.is_empty(), "{name}");
        let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(value["preparationError"], expected, "{name}");
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "{name}: no store connection"
        );
    }
}
