use std::collections::{BTreeMap, VecDeque};

use response_continuation::*;
use serde::Deserialize;

const CONTRACT: &str = include_str!(
    "../../../../pkg/execution/executor/testdata/compatibility/response-continuation-v1.json"
);
const GROUPING: &str =
    include_str!("../../../../pkg/execution/executor/testdata/compatibility/opcode-plan-v1.json");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    schema_version: u8,
    contract: String,
    source: Source,
    defaults: Defaults,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Source {
    commit: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Defaults {
    stored_version: i32,
    response_version: i32,
    attempt: u32,
    max_attempts: u32,
    run_id: String,
    idempotency_key: String,
}
#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct Op {
    op: String,
    id: String,
    name: String,
    display_name: Option<String>,
    data: String,
    options: String,
    prepare_op: String,
    prepare_error: String,
    error: bool,
    user_no_retry: bool,
    mode: String,
    ai: bool,
    prepared: Option<String>,
    pending: bool,
    save_error: String,
}
#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct Case {
    name: String,
    ops: Vec<Op>,
    stored_version: Option<i32>,
    response_version: Option<i32>,
    attempt: Option<u32>,
    max_attempts: Option<u32>,
    force: bool,
    no_retry: bool,
    response_user_no_retry: bool,
    context_mode: String,
    context_coalesce_key: Option<String>,
    retry_at_ns: Option<i64>,
    metadata_error: String,
    pending_error: String,
    reset_error: String,
    queue_error: String,
    repeat: usize,
    tick: bool,
    advance_metadata_ns: i64,
    expected: Expected,
}
#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct Expected {
    events: Vec<String>,
    before: Vec<[String; 2]>,
    writes: BTreeMap<String, String>,
    recipes: Vec<RecipeView>,
    metadata: Vec<MetadataView>,
    pending: Vec<Vec<String>>,
    error: String,
    policy: String,
    lifecycle_ns: Option<Vec<i128>>,
    callback_ns: Option<Vec<i128>>,
    clock_count: Option<usize>,
    handled_at_ns: Option<i128>,
    error_context: String,
    after_enqueue_span: String,
    uuid_sites: BTreeMap<usize, Vec<String>>,
    scheduled: Vec<ScheduledView>,
    stopped_after_prepare: Vec<usize>,
}
#[derive(Deserialize, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "camelCase")]
struct RecipeView {
    branch: usize,
    group: String,
    at_ns: i128,
    #[serde(flatten)]
    payload: PayloadView,
}
#[derive(Deserialize, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "camelCase")]
struct ScheduledView {
    branch: usize,
    name: Option<String>,
    group: String,
    attempt: u32,
    payload: Option<PayloadView>,
}
#[derive(Deserialize, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "camelCase")]
struct PayloadView {
    kind: String,
    job_id: String,
    incoming: String,
    outgoing: String,
    incoming_generator_step: String,
    incoming_generator_step_name: String,
    mode: String,
    coalesce_key: String,
    attempt: u32,
    max_attempts: u32,
    priority: i64,
}
#[derive(Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct MetadataView {
    request_version: i32,
    force_step_plan: bool,
    #[serde(rename = "hasAI")]
    has_ai: bool,
}

fn mode(name: &str) -> ParallelMode {
    match name {
        "race" => ParallelMode::Race,
        "wait" => ParallelMode::Wait,
        "" | "none" => ParallelMode::None,
        _ => panic!("unknown mode"),
    }
}
fn tag(name: &str) -> u8 {
    match name {
        "None" => 0,
        "Step" => 1,
        "StepRun" => 2,
        "StepError" => 3,
        "StepPlanned" => 4,
        "Sleep" => 5,
        "WaitForEvent" => 6,
        "InvokeFunction" => 7,
        "AIGateway" => 8,
        "Gateway" => 9,
        "WaitForSignal" => 10,
        "RunComplete" => 11,
        "StepFailed" => 12,
        "SyncRunComplete" => 13,
        "DiscoveryRequest" => 14,
        "DeferAdd" => 15,
        "DeferAbort" => 16,
        _ => panic!("unknown tag"),
    }
}
fn input(c: &Case, d: &Defaults) -> Input {
    let ops = (0..c.repeat.max(1))
        .flat_map(|_| c.ops.iter())
        .map(|o| {
            Some(Opcode {
                tag: tag(&o.op),
                id: o.id.clone(),
                name: o.name.clone(),
                display_name: o.display_name.clone(),
                data: o.data.as_bytes().to_vec(),
                options: o.options.as_bytes().to_vec(),
                error: o.error.then(|| UserError {
                    name: "E".into(),
                    message: "x".into(),
                    no_retry: o.user_no_retry,
                }),
                parallel_mode: mode(&o.mode),
                has_ai: o.ai,
            })
        })
        .collect();
    Input {
        generation: 1,
        ops,
        config: Config {
            request_version: c.stored_version.unwrap_or(d.stored_version),
            force_step_plan: c.force,
            ..Config::default()
        },
        response_version: c.response_version.unwrap_or(d.response_version),
        response_no_retry: c.no_retry,
        response_user_no_retry: c.response_user_no_retry,
        retry_at: c.retry_at_ns.map(i128::from),
        run_id: d.run_id.clone(),
        idempotency_key: d.idempotency_key.clone(),
        edge: Some(Edge {
            incoming: "function".into(),
            outgoing: "previous".into(),
            ..Edge::default()
        }),
        attempt: c.attempt.unwrap_or(d.attempt),
        max_attempts: c.max_attempts.unwrap_or(d.max_attempts),
        context_mode: mode(&c.context_mode),
        context_group: "original-group".into(),
        context_coalesce_key: c.context_coalesce_key.clone(),
        queue: QueueBindings {
            workspace_id: "00000000-0000-0000-0000-000000000002".into(),
            priority_factor: Some(7),
            ..QueueBindings::default()
        },
        handled_step_error: 8,
        function_overflow: 9,
    }
}

// These symbols are independently mapped to actual Go classes/identities by the
// Go adapter. Neither adapter calls the other or generates expected results.
fn fault(name: &str) -> Option<Fault> {
    let (handle, duplicate, idempotent, state_overflow, always_retry) = match name {
        "" | "none" => return None,
        "already-always" => (11, false, false, false, true),
        "ordinary" => (1, false, false, false, false),
        "overflow" => (2, false, false, true, false),
        "preparation-overflow" => (12, false, false, true, false),
        "wrapped-duplicate" => (3, true, false, false, false),
        "idempotent" => (4, false, true, false, false),
        "metadata" => (5, false, false, false, false),
        "pending" => (6, false, false, false, false),
        "reset" => (7, false, false, false, false),
        "handled" => (8, false, false, false, false),
        "function-overflow" => (9, false, false, false, false),
        "queue-duplicate" => (10, true, false, false, false),
        _ => panic!("unknown error symbol {name}"),
    };
    Some(Fault {
        handle,
        duplicate,
        idempotent,
        state_overflow,
        always_retry,
    })
}
// The fixture supplies the preparation tag independently of retry decisions.
// All other descriptor fields must survive unchanged, including opaque bytes.
fn assert_preparation(opcode: &Opcode, o: &Op) {
    assert!(!o.prepare_op.is_empty(), "missing literal preparation tag");
    assert_eq!(
        opcode,
        &Opcode {
            tag: tag(&o.prepare_op),
            id: o.id.clone(),
            name: o.name.clone(),
            display_name: o.display_name.clone(),
            data: o.data.as_bytes().to_vec(),
            options: o.options.as_bytes().to_vec(),
            error: o.error.then(|| UserError {
                name: "E".into(),
                message: "x".into(),
                no_retry: o.user_no_retry,
            }),
            parallel_mode: mode(&o.mode),
            has_ai: o.ai,
        },
        "exact preparation descriptor (fixture input tag: {})",
        o.op
    );
}

fn payload_view(r: &Recipe) -> PayloadView {
    assert!(!r.passthrough_job_id);
    assert_eq!(
        r.bindings.workspace_id,
        "00000000-0000-0000-0000-000000000002"
    );
    PayloadView {
        kind: match r.kind {
            Kind::Edge => "edge",
            Kind::EdgeError => "edge-error",
        }
        .into(),
        job_id: r.job_id.clone(),
        incoming: r.edge.incoming.clone(),
        outgoing: r.edge.outgoing.clone(),
        incoming_generator_step: r.edge.incoming_generator_step.clone(),
        incoming_generator_step_name: r.edge.incoming_generator_step_name.clone(),
        mode: match r.parallel_mode {
            ParallelMode::None => "none",
            ParallelMode::Wait => "wait",
            ParallelMode::Race => "race",
        }
        .into(),
        coalesce_key: r.parallel_coalesce_key.clone().unwrap_or_default(),
        attempt: r.attempt,
        max_attempts: r.max_attempts,
        priority: r.bindings.priority_factor.unwrap(),
    }
}
#[derive(Default)]
struct PortRecorder {
    events: Vec<String>,
    writes: BTreeMap<String, String>,
    recipes: Vec<RecipeView>,
    metadata: Vec<MetadataView>,
    pending: Vec<Vec<String>>,
    clock_count: usize,
    offset: i128,
    lifecycle: Vec<i128>,
    callbacks: Vec<i128>,
    handling_seen: bool,
    uuid: u128,
    uuid_sites: BTreeMap<usize, Vec<String>>,
    supplied_uuid: BTreeMap<u128, String>,
    scheduled_async: Vec<ScheduledView>,
    scheduled_sync: Vec<ScheduledView>,
    local: Vec<Vec<&'static str>>,
}
impl PortRecorder {
    fn group(&self, group: u128) -> String {
        self.supplied_uuid
            .get(&group)
            .expect("must use a supplied UUID")
            .clone()
    }
    fn history(&self, index: usize, group: &HistoryGroup, parallel: bool) {
        let actual = match group {
            HistoryGroup::Generated(v) => self.group(*v),
            HistoryGroup::Existing(v) => {
                assert_eq!(v, "original-group");
                "original".into()
            }
        };
        assert_eq!(
            actual,
            if parallel {
                format!("history:{index}")
            } else {
                "original".into()
            }
        );
    }
    fn recipe_view(&self, index: usize, recipe: &Recipe) -> RecipeView {
        RecipeView {
            branch: index,
            group: self.group(recipe.group),
            at_ns: recipe.at,
            payload: payload_view(recipe),
        }
    }
    fn scheduled_view(
        &self,
        index: usize,
        item: &ScheduledItem,
        name: &Option<String>,
    ) -> ScheduledView {
        let (group, attempt, payload) = match item {
            ScheduledItem::Recipe(r) => (self.group(r.group), r.attempt, Some(payload_view(r))),
            ScheduledItem::Original {
                history_group,
                attempt,
            } => (
                match history_group {
                    HistoryGroup::Generated(v) => self.group(*v),
                    HistoryGroup::Existing(v) => {
                        assert_eq!(v, "original-group");
                        "original".into()
                    }
                },
                *attempt,
                None,
            ),
        };
        ScheduledView {
            branch: index,
            name: name.clone(),
            group,
            attempt,
            payload,
        }
    }
    fn observe(&mut self, c: &Case, r: &Request) -> Observation {
        let index = match r.key.owner {
            Owner::Response => None,
            Owner::Branch(i) => Some(i as usize),
        };
        let op = index.map(|i| &c.ops[i % c.ops.len()]);
        let id = op.map(|o| o.id.as_str()).unwrap_or("");
        while self.local.len() <= index.unwrap_or(0) {
            self.local.push(Vec::new());
        }
        let branch = index.unwrap_or(0);
        match &r.effect {
            Effect::UpdateMetadata(config) | Effect::ResetMetadata(config) => {
                self.metadata.push(MetadataView {
                    request_version: config.request_version,
                    force_step_plan: config.force_step_plan,
                    has_ai: config.has_ai,
                });
                self.events.push("metadata_update".into());
                self.offset += i128::from(c.advance_metadata_ns);
                Observation::Ack(fault(if matches!(r.effect, Effect::ResetMetadata(_)) {
                    &c.reset_error
                } else {
                    &c.metadata_error
                }))
            }
            Effect::Clock(site) => {
                match site {
                    ClockSite::Handling => self.handling_seen = true,
                    ClockSite::Recipe => {
                        assert!(
                            matches!(
                                self.local[branch].last(),
                                Some(&"uuid:discovery" | &"uuid:planned")
                            ),
                            "UUID before recipe clock"
                        );
                        self.local[branch].push("clock:recipe");
                    }
                    ClockSite::Callback => {
                        assert_eq!(
                            self.local[branch].last(),
                            Some(&"async"),
                            "async launch before callback clock request"
                        );
                        self.local[branch].push("clock:callback");
                    }
                    _ => {}
                }
                let now = self.offset
                    + if c.tick {
                        self.clock_count as i128 * 1_000_000_000
                    } else {
                        0
                    };
                self.clock_count += 1;
                Observation::Clock(now)
            }
            Effect::Uuid(site) => {
                assert!(
                    self.handling_seen,
                    "handling clock before branch activation"
                );
                let site = match site {
                    UuidSite::History => {
                        assert!(
                            self.local[branch].is_empty(),
                            "history UUID before branch activation"
                        );
                        "history"
                    }
                    UuidSite::Discovery => {
                        assert_eq!(
                            self.local[branch].last(),
                            Some(&"release"),
                            "discovery UUID after release"
                        );
                        "discovery"
                    }
                    UuidSite::Planned => {
                        assert!(
                            self.local[branch].is_empty() || self.local[branch] == ["uuid:history"],
                            "planned UUID at branch activation"
                        );
                        "planned"
                    }
                };
                self.uuid_sites.entry(branch).or_default().push(site.into());
                self.local[branch].push(match site {
                    "history" => "uuid:history",
                    "discovery" => "uuid:discovery",
                    _ => "uuid:planned",
                });
                self.uuid += 1;
                self.supplied_uuid
                    .insert(self.uuid, format!("{site}:{branch}"));
                Observation::Uuid(self.uuid)
            }
            Effect::SavePending(ids) => {
                self.pending.push(ids.clone());
                self.events.push(
                    if c.pending_error.is_empty() {
                        "save_pending_completed"
                    } else {
                        "save_pending_failed"
                    }
                    .into(),
                );
                Observation::Ack(fault(&c.pending_error))
            }
            Effect::PrepareSave {
                opcode,
                history_group,
            } => {
                assert_preparation(opcode, op.unwrap());
                self.history(branch, history_group, c.ops.len() * c.repeat.max(1) > 1);
                self.local[branch].push("prepare");
                if let Some(error) = fault(&op.unwrap().prepare_error) {
                    return Observation::Prepared(Err(error));
                }
                Observation::Prepared(Ok(op
                    .unwrap()
                    .prepared
                    .as_ref()
                    .expect("unexpected save preparation; no literal observation supplied")
                    .as_bytes()
                    .to_vec()))
            }
            Effect::PrepareRetrySpan {
                opcode,
                history_group,
            } => {
                assert_preparation(opcode, op.unwrap());
                self.history(branch, history_group, c.ops.len() * c.repeat.max(1) > 1);
                self.local[branch].push("retry_prepare");
                Observation::Done
            }
            Effect::LogExistingOutput(_) => Observation::Done,
            Effect::SaveStep { id, output } => {
                self.local[branch].push("save");
                self.writes
                    .insert(id.clone(), String::from_utf8(output.clone()).unwrap());
                self.events.push(format!("handler:save:{id}"));
                let o = op.unwrap();
                Observation::Saved {
                    has_pending: o.pending,
                    error: fault(&o.save_error),
                }
            }
            Effect::Lifecycle {
                status,
                attempt,
                at,
            } => {
                self.local[branch].push("lifecycle");
                self.lifecycle.push(*at);
                let event = match status {
                    Status::Finished => format!("finished:{id}"),
                    Status::Failed => format!("failed:{id}:Failed:{attempt}"),
                    Status::Errored => format!("failed:{id}:Errored:{attempt}"),
                };
                self.events.push(event);
                Observation::Done
            }
            Effect::ReleaseCapacity => {
                self.local[branch].push("release");
                Observation::Done
            }
            Effect::PreparePlanned { recipe, handled_at } => {
                assert_eq!(self.group(recipe.group), format!("planned:{branch}"));
                if let Some(at) = c.expected.handled_at_ns {
                    assert_eq!(*handled_at, at);
                }
                self.local[branch].push("queue_prepare");
                Observation::Done
            }
            Effect::PrepareDiscovery(recipe) => {
                assert_eq!(self.group(recipe.group), format!("discovery:{branch}"));
                self.local[branch].push("queue_prepare");
                Observation::Done
            }
            Effect::Enqueue(recipe) => {
                self.local[branch].push("enqueue");
                self.events
                    .push(format!("handler:enqueue:{}", recipe.edge.outgoing));
                self.recipes.push(self.recipe_view(branch, recipe));
                Observation::Ack(fault(&c.queue_error))
            }
            Effect::FinishSpan { send } => {
                if !c.expected.after_enqueue_span.is_empty() {
                    self.events
                        .push(format!("span:{}", if *send { "send" } else { "drop" }));
                }
                Observation::Done
            }
            Effect::AsyncScheduled { item, name } => {
                self.local[branch].push("async");
                self.scheduled_async
                    .push(self.scheduled_view(branch, item, name));
                Observation::Done
            }
            Effect::SyncScheduled { item, name, at } => {
                assert_eq!(self.local[branch].last(), Some(&"clock:callback"));
                self.scheduled_sync
                    .push(self.scheduled_view(branch, item, name));
                self.local[branch].push("callback");
                self.callbacks.push(*at);
                let (kind, attempt) = match item {
                    ScheduledItem::Recipe(r) => (
                        match r.kind {
                            Kind::Edge => "edge",
                            Kind::EdgeError => "edge-error",
                        },
                        r.attempt,
                    ),
                    ScheduledItem::Original { attempt, .. } => ("", *attempt),
                };
                self.events.push(format!(
                    "scheduled:{kind}:{}:{attempt}",
                    name.as_deref().unwrap_or("<nil>")
                ));
                Observation::Done
            }
        }
    }
}

#[test]
fn independent_shared_literal_contract() {
    let f: Fixture = serde_json::from_str(CONTRACT).unwrap();
    assert_eq!(f.schema_version, 1);
    assert_eq!(f.contract, "response-continuation-v1");
    assert_eq!(f.source.commit, "327acb5f22e4499cdfc4363b4465ce5ebb58d88d");
    for c in &f.cases {
        // Exercise two sibling delivery orders against the same independent
        // literal partial order; no severity-based winner is manufactured.
        for reverse in [false, true] {
            let (mut session, mut progress) = Session::start(input(c, &f.defaults)).unwrap();
            let original = session.original_attempt();
            let mut ports = PortRecorder::default();
            let mut ready = VecDeque::from(progress.ready);
            let mut calls = 0;
            while progress.completion.is_none() {
                let r = if reverse {
                    ready.pop_back()
                } else {
                    ready.pop_front()
                }
                .unwrap_or_else(|| panic!("no progress in {}", c.name));
                let observation = ports.observe(c, &r);
                progress = session.resume(r.key, observation).unwrap();
                for request in &progress.ready {
                    if request.effect == Effect::Clock(ClockSite::Callback) {
                        let Owner::Branch(index) = request.key.owner else {
                            panic!("callback clock must belong to a branch")
                        };
                        assert_eq!(
                            ports.local[index as usize].last(),
                            Some(&"async"),
                            "launch observed before callback clock is requested"
                        );
                    }
                }
                ready.extend(progress.ready.clone());
                calls += 1;
                assert!(calls < 100_000);
                assert!(
                    session.outstanding().count() <= c.ops.len() + 1,
                    "bounded outstanding requests"
                );
            }
            assert!(ready.is_empty(), "must join every sibling in {}", c.name);
            assert_eq!(session.outstanding().count(), 0);
            assert_eq!(session.original_attempt(), original);
            let e = &c.expected;
            let result = progress.completion.unwrap();
            if e.error == "none" {
                assert_eq!(result, None, "{}", c.name);
            } else {
                let result = result.unwrap();
                assert!(
                    e.error
                        .split('|')
                        .any(|s| fault(s).unwrap().handle == result.fault.handle),
                    "{}: {result:?}",
                    c.name
                );
                let symbol = e
                    .error
                    .split('|')
                    .find(|s| fault(s).unwrap().handle == result.fault.handle)
                    .unwrap();
                assert_eq!(
                    result.fault,
                    fault(symbol).unwrap(),
                    "original handle and classifications in {}",
                    c.name
                );
                let policy = match e.policy.as_str() {
                    "never" => RetryPolicy::Never,
                    "at" => RetryPolicy::At(i128::from(c.retry_at_ns.unwrap())),
                    "always" => RetryPolicy::Always,
                    _ => RetryPolicy::Original,
                };
                assert_eq!(result.policy, policy, "{}", c.name);
                let context = match result.context {
                    ErrorContext::Original => "original",
                    ErrorContext::Metadata => "metadata",
                    ErrorContext::Pending => "pending",
                    ErrorContext::Reset => "reset",
                    ErrorContext::Discovery => "discovery",
                };
                assert_eq!(context, e.error_context, "{}", c.name);
            }
            let mut actual = ports
                .events
                .iter()
                .filter(|v| !v.starts_with("span:"))
                .cloned()
                .collect::<Vec<_>>();
            actual.sort();
            let mut expected = e.events.clone();
            expected.sort();
            assert_eq!(actual, expected, "{}", c.name);
            for [a, b] in &e.before {
                assert_before(&ports.events, a, b);
            }
            assert_eq!(
                ports.uuid_sites, e.uuid_sites,
                "UUID sites and per-branch activation order: {}",
                c.name
            );
            ports.scheduled_async.sort();
            ports.scheduled_sync.sort();
            let mut scheduled = e.scheduled.iter().collect::<Vec<_>>();
            scheduled.sort();
            assert_eq!(
                ports.scheduled_async.iter().collect::<Vec<_>>(),
                scheduled,
                "async payload/delivery/suppression: {}",
                c.name
            );
            assert_eq!(
                ports.scheduled_sync.iter().collect::<Vec<_>>(),
                scheduled,
                "sync payload/bindings: {}",
                c.name
            );
            assert_eq!(ports.writes, e.writes, "{}", c.name);
            assert_eq!(ports.metadata, e.metadata, "{}", c.name);
            assert_eq!(ports.pending, e.pending, "{}", c.name);
            ports.recipes.sort();
            let mut expected = e.recipes.iter().collect::<Vec<_>>();
            expected.sort();
            assert_eq!(
                ports.recipes.iter().collect::<Vec<_>>(),
                expected,
                "{}",
                c.name
            );
            if let Some(n) = e.clock_count {
                assert_eq!(ports.clock_count, n, "{}", c.name);
            }
            if let Some(v) = &e.lifecycle_ns {
                assert_eq!(&ports.lifecycle, v, "{}", c.name);
            }
            if let Some(v) = &e.callback_ns {
                assert_eq!(&ports.callbacks, v, "{}", c.name);
            }
            if !e.after_enqueue_span.is_empty() {
                assert!(
                    ports
                        .events
                        .contains(&format!("span:{}", e.after_enqueue_span))
                );
            }
            for branch in &e.stopped_after_prepare {
                assert_eq!(
                    ports.local[*branch],
                    ["prepare"],
                    "no effects after failed preparation: {}",
                    c.name
                );
            }
            for sequence in &ports.local {
                if let Some(release) = sequence.iter().position(|v| *v == "release") {
                    let save = sequence.iter().position(|v| *v == "save").unwrap();
                    let lifecycle = sequence.iter().position(|v| *v == "lifecycle").unwrap();
                    assert!(save < lifecycle && lifecycle < release);
                }
            }
        }
    }
}
fn assert_before(events: &[String], a: &str, b: &str) {
    assert!(
        events.iter().rposition(|v| v == a).unwrap() < events.iter().position(|v| v == b).unwrap(),
        "{a} before {b}: {events:?}"
    );
}

#[test]
fn accepted_grouping_and_hash_vectors_are_reused() {
    let fixture: serde_json::Value = serde_json::from_str(GROUPING).unwrap();
    for c in fixture["coalesceHashCases"].as_array().unwrap() {
        let ids = c["stepIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            coalesce_key(c["runId"].as_str().unwrap(), &ids),
            c["expectedKey"].as_str().unwrap()
        );
    }
    // The kernel still independently owns classification for all seventeen tags.
    for c in fixture["cases"].as_array().unwrap() {
        let tags = c["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| opcode_kernel::OpcodeTag::try_from(tag(v["op"].as_str().unwrap())).unwrap())
            .collect::<Vec<_>>();
        let p = opcode_kernel::project(&tags).unwrap();
        let expected = &c["expected"];
        assert_eq!(
            serde_json::to_value(&p.priority_indices).unwrap(),
            expected["priorityIndices"]
        );
        assert_eq!(
            serde_json::to_value(&p.other_indices).unwrap(),
            expected["otherIndices"]
        );
        assert_eq!(
            serde_json::to_value(&p.non_lazy_indices).unwrap(),
            expected["nonLazyIndices"]
        );
        assert_eq!(p.history_group, expected["historyGroup"].as_bool().unwrap());
        assert_eq!(p.has_plan_op, expected["hasPlanOp"].as_bool().unwrap());
    }
}

fn sample(name: &str) -> (Input, Case) {
    let mut f: Fixture = serde_json::from_str(CONTRACT).unwrap();
    let index = f.cases.iter().position(|c| c.name == name).unwrap();
    let c = f.cases.remove(index);
    (input(&c, &f.defaults), c)
}
fn sole(p: &Progress) -> Request {
    assert_eq!(p.ready.len(), 1);
    p.ready[0].clone()
}
fn respond(s: &mut Session, r: &Request, o: Observation) -> Progress {
    s.resume(r.key, o).unwrap()
}

#[test]
fn whole_batch_rejects_before_effects() {
    let (base, _) = sample("eligible-six-tags");
    for excluded in [5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 255] {
        let mut i = base.clone();
        i.ops.last_mut().unwrap().as_mut().unwrap().tag = excluded;
        assert_eq!(Session::start(i).unwrap_err(), Reject::UnsupportedBatch);
    }
    let mut i = base.clone();
    i.ops.push(None);
    assert_eq!(Session::start(i).unwrap_err(), Reject::InvalidInput);
    let mut i = base;
    i.ops[3].as_mut().unwrap().error = None;
    assert_eq!(Session::start(i).unwrap_err(), Reject::InvalidInput);
}

#[test]
fn stale_duplicate_and_mismatched_resume_preserve_outstanding_state() {
    let (mut i, _) = sample("success-recipe-clocks");
    i.context_group = "original-group".into();
    let (mut s, p) = Session::start(i).unwrap();
    let r = sole(&p);
    let original = s.outstanding().cloned().collect::<Vec<_>>();
    let mut stale = r.key;
    stale.generation += 1;
    let mut wrong_index = r.key;
    wrong_index.owner = Owner::Branch(0);
    let mut wrong_sequence = r.key;
    wrong_sequence.sequence += 1;
    for key in [stale, wrong_index, wrong_sequence] {
        assert_eq!(
            s.resume(key, Observation::Clock(0)),
            Err(Reject::UnknownRequest)
        );
        assert_eq!(s.outstanding().cloned().collect::<Vec<_>>(), original);
    }
    assert_eq!(
        s.resume(r.key, Observation::Done),
        Err(Reject::MismatchedResult)
    );
    assert_eq!(s.outstanding().cloned().collect::<Vec<_>>(), original);
    let p = respond(&mut s, &r, Observation::Clock(0));
    assert_eq!(
        s.resume(r.key, Observation::Clock(0)),
        Err(Reject::UnknownRequest)
    );
    let prep = sole(&p);
    assert!(
        matches!(&prep.effect,Effect::PrepareSave{history_group:HistoryGroup::Existing(group),..} if group=="original-group")
    );
    let p = respond(&mut s, &prep, Observation::Prepared(Ok(b"owned".to_vec())));
    let save = sole(&p);
    assert!(matches!(save.effect,Effect::SaveStep{ref output,..}if output==b"owned"));
    assert_eq!(
        s.resume(
            save.key,
            Observation::Saved {
                has_pending: false,
                error: Some(Fault::default())
            }
        ),
        Err(Reject::MismatchedResult)
    );
    let p = respond(
        &mut s,
        &save,
        Observation::Saved {
            has_pending: false,
            error: None,
        },
    );
    assert!(matches!(
        sole(&p).effect,
        Effect::Clock(ClockSite::Lifecycle)
    ));
}

#[test]
fn sibling_can_enqueue_while_other_save_is_outstanding_and_join_waits() {
    let (mut i, _) = sample("eligible-six-tags");
    i.ops = vec![i.ops[2].clone(), i.ops[2].clone()];
    i.config.force_step_plan = true;
    let (mut s, p) = Session::start(i).unwrap();
    let handling = sole(&p);
    let p = respond(&mut s, &handling, Observation::Clock(0));
    assert_eq!(p.ready.len(), 2);
    let mut saves = Vec::new();
    for history in p.ready {
        let p = respond(
            &mut s,
            &history,
            Observation::Uuid(
                100 + match history.key.owner {
                    Owner::Branch(i) => u128::from(i),
                    _ => unreachable!(),
                },
            ),
        );
        let p = respond(
            &mut s,
            &sole(&p),
            Observation::Prepared(Ok(b"output".to_vec())),
        );
        saves.push(sole(&p));
    }
    assert_ne!(
        saves[0].key, saves[1].key,
        "duplicate IDs retain distinct original indices"
    );
    let mut p = respond(
        &mut s,
        &saves[0],
        Observation::Saved {
            has_pending: false,
            error: None,
        },
    );
    let mut enqueued = false;
    while !p.ready.is_empty() {
        let r = sole(&p);
        let o = match r.effect {
            Effect::Clock(_) => Observation::Clock(0),
            Effect::Uuid(_) => Observation::Uuid(999),
            Effect::Enqueue(_) => {
                enqueued = true;
                Observation::Ack(None)
            }
            Effect::ResetMetadata(_) => Observation::Ack(None),
            _ => Observation::Done,
        };
        p = respond(&mut s, &r, o);
        assert!(p.completion.is_none());
    }
    assert!(enqueued);
    assert_eq!(
        s.outstanding().cloned().collect::<Vec<_>>(),
        vec![saves[1].clone()]
    );
    let p = respond(
        &mut s,
        &saves[1],
        Observation::Saved {
            has_pending: false,
            error: fault("ordinary"),
        },
    );
    assert_eq!(p.completion.unwrap().unwrap().fault.handle, 1);
}

#[test]
fn competing_errors_keep_observed_first_without_ranking_or_cancellation() {
    for first in [0, 1] {
        let (i, _) = sample("competing-errors-complete-join");
        let (mut s, p) = Session::start(i).unwrap();
        let p = respond(&mut s, &sole(&p), Observation::Clock(0));
        let mut saves = Vec::new();
        for history in p.ready {
            let p = respond(
                &mut s,
                &history,
                Observation::Uuid(
                    100 + u128::from(history.key.sequence)
                        + match history.key.owner {
                            Owner::Branch(i) => u128::from(i),
                            _ => 0,
                        },
                ),
            );
            let p = respond(&mut s, &sole(&p), Observation::Prepared(Ok(vec![])));
            saves.push(sole(&p));
        }
        let errors = [fault("ordinary"), fault("overflow")];
        let p = respond(
            &mut s,
            &saves[first],
            Observation::Saved {
                has_pending: false,
                error: errors[first].clone(),
            },
        );
        assert!(p.completion.is_none());
        let p = respond(
            &mut s,
            &saves[1 - first],
            Observation::Saved {
                has_pending: false,
                error: errors[1 - first].clone(),
            },
        );
        assert_eq!(
            p.completion.unwrap().unwrap().fault,
            errors[first].clone().unwrap()
        );
    }
}
