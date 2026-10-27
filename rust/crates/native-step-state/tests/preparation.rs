use native_step_state::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    name: String,
    data: String,
    prepared: String,
    bytes: usize,
}
fn config() -> PreparationConfig {
    PreparationConfig {
        request_version: 2,
        force_step_plan: false,
        trace_disabled: true,
        has_ai: false,
        sdk_metadata: vec![],
        experiments: vec![],
        loaded_state_bytes: 34,
        state_limit: Some(0),
    }
}
fn response(data: &[u8]) -> Vec<u8> {
    let mut raw = b"[{\"op\":\"StepRun\",\"id\":\"s\"".to_vec();
    if !data.is_empty() {
        raw.extend_from_slice(b",\"data\":");
        raw.extend_from_slice(data);
    }
    raw.extend_from_slice(b"}]");
    raw
}
#[test]
fn shared_go_literal_preparation() {
    let f: Fixture = serde_json::from_slice(include_bytes!(
        "../../../../pkg/execution/executor/testdata/compatibility/native-step-preparation-v1.json"
    ))
    .unwrap();
    for case in f.cases {
        let prepared = prepare_response(&response(case.data.as_bytes()), &config()).unwrap();
        assert_eq!(prepared.output(), case.prepared.as_bytes(), "{}", case.name);
        assert_eq!(prepared.output().len(), case.bytes, "{}", case.name);
    }
}
#[test]
fn admission_and_size_precede_effects() {
    let mut c = config();
    c.state_limit = Some(47);
    assert_eq!(
        prepare_response(&response(b""), &c).unwrap().output(),
        b"{\"data\":null}"
    );
    c.state_limit = Some(46);
    assert!(matches!(
        prepare_response(&response(b""), &c),
        Err(PreparationError::ErrStateOverflowed { limit: 46 })
    ));
    c.state_limit = Some(0);
    for raw in [
        b"[]".as_slice(),
        b"[{\"op\":\"Step\",\"id\":\"s\"}]",
        b"[{\"op\":\"StepRun\",\"id\":\"s\",\"opts\":{\"type\":\"step.ai.wrap\"}}]",
        b"[{\"op\":\"StepRun\",\"id\":\"s\",\"metadata\":[{}]}]",
        b"[{\"op\":\"StepRun\",\"id\":\"s\",\"timing\":{}}]",
        b"[{\"op\":\"StepRun\",\"id\":\"s\",\"unknown\":null}]",
        b"[{\"op\":\"StepRun\",\"id\":\"s\"},{\"op\":\"StepRun\",\"id\":\"t\"}]",
        b"[{\"op\":\"StepRun\",\"id\":\"s\",\"id\":\"t\"}]",
    ] {
        assert!(matches!(
            prepare_response(raw, &c),
            Err(PreparationError::Ineligible)
        ));
    }
    c.trace_disabled = false;
    assert!(matches!(
        prepare_response(&response(b"null"), &c),
        Err(PreparationError::Ineligible)
    ));
    c.trace_disabled = true;
    assert!(matches!(
        prepare_response(&response(br#"{"totalUsage":{}}"#), &c),
        Err(PreparationError::Ineligible)
    ));
    let mut data = vec![b'"'];
    data.resize(MAX_STEP_OUTPUT - 10, b'x');
    data.push(b'"');
    assert_eq!(
        prepare_response(&response(&data), &c)
            .unwrap()
            .output()
            .len(),
        MAX_STEP_OUTPUT
    );
    data.insert(1, b'x');
    assert!(matches!(
        prepare_response(&response(&data), &c),
        Err(PreparationError::ErrStepOutputTooLarge)
    ));
}
#[test]
fn go_raw_message_scanner_edges() {
    for (data, output) in [
        (b"1e9999".as_slice(), b"{\"data\":1e9999}".as_slice()),
        (b"{\"a\":1,\"a\":2}", b"{\"data\":{\"a\":1,\"a\":2}}"),
        (b"\"\xff\\ud800\"", b"{\"data\":\"\xff\\ud800\"}"),
        (
            "\"<>&\u{2028}\u{2029}\"".as_bytes(),
            b"{\"data\":\"\\u003c\\u003e\\u0026\\u2028\\u2029\"}",
        ),
    ] {
        assert_eq!(
            prepare_response(&response(data), &config())
                .unwrap()
                .output(),
            output
        );
    }
    for data in [
        b"01".as_slice(),
        b"1.",
        b"+1",
        b"--1",
        b"[1,]",
        b"{\"a\":1,}",
        b"\"\n\"",
        b"null true",
        b"\"\\u000x\"",
    ] {
        assert!(matches!(
            prepare_response(&response(data), &config()),
            Err(PreparationError::InvalidJson)
        ));
    }
    // The accepted response array/object consume two levels of Go's depth cap.
    let data = format!("{}0{}", "[".repeat(9998), "]".repeat(9998));
    assert!(prepare_response(&response(data.as_bytes()), &config()).is_ok());
    let data = format!("[{data}]");
    assert!(matches!(
        prepare_response(&response(data.as_bytes()), &config()),
        Err(PreparationError::InvalidJson)
    ));
}

#[test]
fn cancellation_stops_admission_without_a_client_attempt() {
    use std::{sync::atomic::Ordering, time::Duration};
    let config: StateConfig = serde_json::from_str(r#"{"prefix":"estate","layout":"NeverShard","identity":{"runId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","functionId":"00000000-0000-0000-0000-000000000001","accountId":"00000000-0000-0000-0000-000000000003","envId":"00000000-0000-0000-0000-000000000002","appId":"00000000-0000-0000-0000-000000000004"}}"#).unwrap();
    let request = SaveRequest::new(&config, b"s".to_vec(), b"opaque".to_vec(), 0).unwrap();
    let store: StoreConfig = serde_json::from_str(r#"{"address":"127.0.0.1:1","password":"unused","connectTimeoutMs":50,"readTimeoutMs":50,"writeTimeoutMs":50}"#).unwrap();
    let mut runtime = Runtime::new(store).unwrap();
    let ctx = CallContext::new(Duration::from_secs(1));
    ctx.cancellation().store(true, Ordering::Release);
    let result = runtime.save_step(&ctx, &request, false);
    assert_eq!(result.error, ErrorClass::Stopped);
    assert_eq!(result.attempts, 0);
    assert_eq!(result.write_status, WriteStatus::NotSent);
}
