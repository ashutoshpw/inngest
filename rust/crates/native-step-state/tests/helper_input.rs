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
