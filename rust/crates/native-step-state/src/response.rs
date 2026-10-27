//! One invocation in a constructed, exclusive test domain. This certificate is
//! checked against actual test setup before projection; it is no runtime selector.
use crate::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Read,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Deserialize, PartialEq, Eq)]
pub enum Activity {
    Absent,
    Present,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Domain {
    pub observer_registrations: usize,
    pub tracing: Activity,
    pub capacity_lease: Activity,
    pub metadata_writers: usize,
    pub response_owners: usize,
    pub metadata_generation: Activity,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Loaded {
    pub request_version: i32,
    pub force_step_plan: bool,
    pub state_bytes: usize,
    pub metadata_size: i64,
    pub metadata_size_loaded: i64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ResponseInput {
    pub domain: Domain,
    pub response: Vec<u8>,
    pub response_version: i32,
    pub no_retry: bool,
    pub retry_at: Option<String>,
    pub loaded: Loaded,
    /// A static setting only: None is absent, Some(0) uses Go's default.
    pub state_limit: Option<usize>,
    pub queue: QueueConfig,
    pub idempotency_key: String,
    pub incoming: String,
    pub priority_factor: Option<i64>,
    pub identifier_priority_factor: Option<i64>,
    pub max_attempts: Option<u32>,
    pub function_version: u32,
    pub event_id: String,
    pub event_ids: Vec<String>,
}
#[derive(Debug, Serialize, PartialEq, Eq)]
pub enum Disposition {
    Success,
    Error,
    NeverRetry,
    RetryAt,
    AlwaysRetry,
    StateOverflow,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Outcome {
    pub disposition: Disposition,
    pub retry_at: Option<String>,
    pub preparation_error: Option<PreparationError>,
    pub save: Option<SaveOutcome>,
    pub enqueue: Option<EnqueueOutcome>,
    pub metadata_size_loaded: i64,
    pub group_id: Option<String>,
    pub now_ms: Option<i64>,
    pub failure: Option<&'static str>,
}
impl ResponseInput {
    fn admit(&self, state: &StateConfig) -> Result<(), PreparationError> {
        let d = &self.domain;
        if d.observer_registrations != 0
            || d.tracing != Activity::Absent
            || d.capacity_lease != Activity::Absent
            || d.metadata_writers != 1
            || d.response_owners != 1
            || d.metadata_generation != Activity::Absent
            || self.loaded.request_version != 2
            || self.loaded.force_step_plan
            || self.event_ids.len() != 1
        {
            return Err(PreparationError::Ineligible);
        }
        state.keys()?;
        if self.loaded.metadata_size < 0
            || self.loaded.metadata_size_loaded < 0
            || self.queue.prefix.is_empty()
            || self.queue.prefix.contains(['{', '}'])
            || self.incoming.is_empty()
            || self.event_ids[0] != self.event_id
            || crate::enqueue::ulid_time(&self.event_id).is_none()
            || state.identity.function_id == "00000000-0000-0000-0000-000000000000"
        {
            return Err(PreparationError::InvalidConfiguration);
        }
        let priority = self
            .priority_factor
            .or(self.identifier_priority_factor)
            .unwrap_or(0)
            .checked_mul(1000)
            .ok_or(PreparationError::InvalidConfiguration)?;
        let start = crate::enqueue::ulid_time(&state.identity.run_id).unwrap();
        // Source-only score validation precedes sockets or generated bindings.
        if start
            .checked_sub(priority)
            .is_none_or(|n| n.unsigned_abs() > (1_u64 << 53) - 1)
        {
            return Err(PreparationError::InvalidConfiguration);
        }
        Ok(())
    }
}
// Standard-library Linux facility: no new crate, UUID stack, injected group,
// generated carrier or caller clock. Runtime qualification is Linux amd64 only.
fn group() -> std::io::Result<String> {
    let mut bytes = [0_u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    bytes[6] = (bytes[6] & 15) | 0x40;
    bytes[8] = (bytes[8] & 63) | 0x80;
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    ))
}

pub fn run(
    input: ResponseInput,
    state: StateConfig,
    store: StoreConfig,
    budget_ms: u64,
    disabled_retries: bool,
) -> Outcome {
    run_with_clock(
        input,
        state,
        store,
        budget_ms,
        disabled_retries,
        &mut || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| PreparationError::InvalidConfiguration)
        },
    )
}
fn run_with_clock(
    input: ResponseInput,
    state: StateConfig,
    store: StoreConfig,
    budget_ms: u64,
    disabled_retries: bool,
    clock: &mut impl FnMut() -> Result<Duration, PreparationError>,
) -> Outcome {
    // Retain response facts, but finalize policy only after the invocation's
    // preparation/save/conditional-enqueue result. No Go policy handoff follows.
    let no_retry = input.no_retry;
    let retry_at = input.retry_at.clone();
    let mut out = execute(input, state, store, budget_ms, disabled_retries, clock);
    if out.disposition == Disposition::Error {
        if no_retry {
            out.disposition = Disposition::NeverRetry;
        } else if retry_at.is_some() {
            out.disposition = Disposition::RetryAt;
            out.retry_at = retry_at;
        }
    }
    out
}
fn execute(
    input: ResponseInput,
    state: StateConfig,
    store: StoreConfig,
    budget_ms: u64,
    disabled_retries: bool,
    clock: &mut impl FnMut() -> Result<Duration, PreparationError>,
) -> Outcome {
    let mut out = Outcome {
        disposition: Disposition::Error,
        retry_at: None,
        preparation_error: None,
        save: None,
        enqueue: None,
        metadata_size_loaded: input.loaded.metadata_size_loaded,
        group_id: None,
        now_ms: None,
        failure: None,
    };
    let prepared = input.admit(&state).and_then(|()| {
        prepare_response(
            &input.response,
            &PreparationConfig {
                request_version: 2,
                force_step_plan: false,
                trace_disabled: true,
                has_ai: false,
                sdk_metadata: vec![],
                experiments: vec![],
                loaded_state_bytes: input.loaded.state_bytes,
                state_limit: input.state_limit,
            },
        )
    });
    let step = match prepared {
        Ok(step) => step,
        Err(error) => {
            if matches!(error, PreparationError::ErrStateOverflowed { .. }) {
                out.disposition = Disposition::StateOverflow;
                out.retry_at = None;
            }
            out.preparation_error = Some(error);
            return out;
        }
    };
    let mut runtime = match Runtime::new(store) {
        Ok(r) => r,
        Err(error) => {
            out.preparation_error = Some(error);
            return out;
        }
    };
    let ctx = CallContext::new(Duration::from_millis(budget_ms));
    // Exclusive ownership replaces Go's atomic swap only in this test domain.
    // Advance even for a nonpositive difference, and only after preparation.
    let delta = (input.loaded.metadata_size - input.loaded.metadata_size_loaded).max(0);
    out.metadata_size_loaded = input.loaded.metadata_size;
    let request = SaveRequest::new(
        &state,
        step.step_id().as_bytes().to_vec(),
        step.output().to_vec(),
        delta,
    )
    .unwrap();
    let saved = runtime.save_step(&ctx, &request, disabled_retries);
    let accepted = matches!(
        saved.error,
        ErrorClass::None | ErrorClass::ErrDuplicateResponse | ErrorClass::ErrIdempotentResponse
    );
    let pending = saved.has_pending;
    out.save = Some(saved);
    if !accepted {
        return out;
    }
    // These bindings activate after accepted save, even when pending suppresses
    // enqueue. Neither NotApplied duplicates nor pending are post-write rejection.
    out.group_id = match group() {
        Ok(id) => Some(id),
        Err(_) => {
            out.failure = Some("group-generation");
            return out;
        }
    };
    out.now_ms = clock().ok().and_then(|d| i64::try_from(d.as_millis()).ok());
    let Some(now) = out.now_ms else {
        out.failure = Some("clock");
        return out;
    };
    if !pending {
        let discovery = DiscoveryConfig {
            queue: input.queue,
            save_completed: true,
            has_pending: false,
            idempotency_key: input.idempotency_key,
            incoming: input.incoming,
            group_id: out.group_id.clone().unwrap(),
            carrier: None,
            now_ms: now,
            priority_factor: input.priority_factor,
            identifier_priority_factor: input.identifier_priority_factor,
            max_attempts: input.max_attempts,
            function_version: input.function_version,
            event_id: input.event_id,
            event_ids: input.event_ids,
        };
        let request = match EnqueueRequest::with_clock(&state, &step, &discovery, clock) {
            Ok(r) => r,
            Err(_) => {
                out.failure = Some("discovery-construction");
                return out;
            }
        };
        let enqueued = runtime.enqueue(&ctx, &request);
        let success = matches!(
            enqueued.class,
            EnqueueClass::Enqueued | EnqueueClass::Duplicate
        );
        out.enqueue = Some(enqueued);
        if !success {
            out.disposition = Disposition::AlwaysRetry;
            out.retry_at = None;
            return out;
        }
    }
    out.disposition = Disposition::Success;
    out.retry_at = None;
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Executed by the Go store-owning test with its authenticated helper store.
    // This private unit-test boundary adds no descriptor field to the owner.
    #[test]
    #[ignore = "requires the authenticated store prepared by TestResponseOneShotClockStages"]
    fn advancing_owner_store() {
        let raw = std::io::read_to_string(std::io::stdin()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let descriptor = &value["descriptor"];
        let case = value["case"].as_str().unwrap();
        let times: &[u64] = match case {
            "pending" => &[1700000000990],
            "future-producer" => &[1700000000990, 1699999998000, 1699999999005],
            "future-boundary" => &[1700000000990, 1699999998990, 1700000001005, 1700000001009],
            "equal-ms" => &[1700000000990, 1700000000990, 1700000000990],
            "sub-ms" => &[1700000000990, 1700000000990, 1700000000990, 1700000001009],
            _ => &[1700000000990, 1700000000995, 1700000001005, 1700000001009],
        };
        let mut reads = 0;
        let mut clock = || {
            let at = times.get(reads).expect("unexpected clock stage");
            reads += 1;
            let mut time = Duration::from_millis(*at);
            if case == "sub-ms" && reads == 3 {
                time += Duration::from_nanos(1);
            }
            Ok(time)
        };
        let out = run_with_clock(
            serde_json::from_value(descriptor["operation"]["input"].clone()).unwrap(),
            serde_json::from_value(descriptor["state"].clone()).unwrap(),
            serde_json::from_value(descriptor["store"].clone()).unwrap(),
            5000,
            true,
            &mut clock,
        );
        assert_eq!(out.disposition, Disposition::Success);
        assert_eq!(
            reads,
            times.len(),
            "pending must not sample enqueue clocks; future/equal scores must not sample pointer clocks"
        );
        println!("CLOCK_OUTCOME {}", serde_json::to_string(&out).unwrap());
        if case != "pending" {
            let store: StoreConfig = serde_json::from_value(descriptor["store"].clone()).unwrap();
            let client =
                redis::Client::open((store.address.ip().to_string(), store.address.port()))
                    .unwrap();
            let mut reader = client.get_connection().unwrap();
            redis::cmd("AUTH")
                .arg(&store.password)
                .query::<()>(&mut reader)
                .unwrap();
            let before: std::collections::BTreeMap<String, String> = redis::cmd("HGETALL")
                .arg("{queue}:queue:item")
                .query(&mut reader)
                .unwrap();
            let mut reads = 0;
            let later = run_with_clock(
                serde_json::from_value(descriptor["operation"]["input"].clone()).unwrap(),
                serde_json::from_value(descriptor["state"].clone()).unwrap(),
                store,
                5000,
                true,
                &mut || {
                    let at = times.get(reads).expect("unexpected duplicate clock stage");
                    reads += 1;
                    let mut time = Duration::from_millis(*at + 10000);
                    if case == "sub-ms" && reads == 3 {
                        time += Duration::from_nanos(1);
                    }
                    Ok(time)
                },
            );
            assert_eq!(later.disposition, Disposition::Success);
            assert_eq!(later.save.unwrap().error, ErrorClass::ErrIdempotentResponse);
            assert_eq!(later.enqueue.unwrap().class, EnqueueClass::Duplicate);
            assert_eq!(reads, times.len());
            let after: std::collections::BTreeMap<String, String> = redis::cmd("HGETALL")
                .arg("{queue}:queue:item")
                .query(&mut reader)
                .unwrap();
            assert_eq!(
                before, after,
                "duplicate must preserve the first persisted timestamps"
            );
        }
    }
}
