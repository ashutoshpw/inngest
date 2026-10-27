//! Isolated whole-response continuation model. No host, I/O, callbacks or routing.
//!
//! Start owns one response; Resume accepts one correlated external observation.
//! The host executes ready requests outside these calls and retains contexts,
//! prepared metadata accounting, spans and original errors. This API is a model,
//! not a stable ABI. Never fall back after executing any request.

use serde::{Deserialize, Serialize};

use opcode_kernel::{MAX_OPCODES, OpcodeTag};
use xxhash_rust::xxh64::Xxh64;

pub const STEP_LIMIT: usize = 1_000;
pub type Time = i128; // Unix nanoseconds; no rounding at the model boundary.
pub type ErrorHandle = u64;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub request_version: i32,
    pub force_step_plan: bool,
    pub started_at: Time,
    pub has_ai: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UserError {
    pub name: String,
    pub message: String,
    pub no_retry: bool,
}

/// Raw bytes/options remain owned data. Decoding and output/trace preparation
/// belong to Go; parallel_mode/has_ai are observations, never continuation decisions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Opcode {
    pub tag: u8,
    pub id: String,
    pub name: String,
    pub display_name: Option<String>,
    pub data: Vec<u8>,
    pub options: Vec<u8>,
    pub error: Option<UserError>,
    pub parallel_mode: ParallelMode,
    pub has_ai: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub incoming: String,
    pub outgoing: String,
    pub incoming_generator_step: String,
    pub incoming_generator_step_name: String,
}

/// Owned observations copied into queue recipes. Go materializes Identifier,
/// concurrency and semaphore values; Rust makes no decisions inside those bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueBindings {
    pub workspace_id: String,
    pub identifier: Vec<u8>,
    pub priority_factor: Option<i64>,
    pub custom_concurrency_keys: Vec<u8>,
    pub step_semaphores: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Input {
    pub generation: u64,
    pub ops: Vec<Option<Opcode>>,
    pub config: Config,
    pub response_version: i32,
    pub response_no_retry: bool,
    pub response_user_no_retry: bool,
    pub retry_at: Option<Time>,
    pub run_id: String,
    pub idempotency_key: String,
    pub edge: Option<Edge>,
    pub attempt: u32,
    pub max_attempts: u32,
    pub context_mode: ParallelMode,
    pub context_group: String,
    pub context_coalesce_key: Option<String>,
    pub queue: QueueBindings,
    /// Go's original ErrHandledStepError and standard function-limit error.
    pub handled_step_error: ErrorHandle,
    pub function_overflow: ErrorHandle,
}

/// Classifications are observations of Go errors.Is/As. Identity is an opaque
/// adapter-owned handle; this crate never reconstructs an error message.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Fault {
    pub handle: ErrorHandle,
    pub duplicate: bool,
    pub idempotent: bool,
    pub state_overflow: bool,
    pub always_retry: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ErrorContext {
    Original,
    Metadata,
    Pending,
    Reset,
    Discovery,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetryPolicy {
    Original,
    Never,
    At(Time),
    Always,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Disposition {
    pub fault: Fault,
    pub context: ErrorContext,
    pub policy: RetryPolicy,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ParallelMode {
    #[default]
    None,
    Wait,
    Race,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Kind {
    Edge,
    EdgeError,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Recipe {
    pub job_id: String,
    pub group: u128,
    pub kind: Kind,
    pub edge: Edge,
    pub at: Time,
    pub attempt: u32,
    pub max_attempts: u32,
    pub parallel_mode: ParallelMode,
    pub parallel_coalesce_key: Option<String>,
    pub bindings: QueueBindings,
    pub passthrough_job_id: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClockSite {
    Handling,
    Lifecycle,
    Recipe,
    Callback,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum UuidSite {
    History,
    Discovery,
    Planned,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Status {
    Finished,
    Failed,
    Errored,
}
/// Existing queue group IDs can be empty or arbitrary strings. Only parallel
/// branch history groups request fresh UUID bits from the external binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HistoryGroup {
    Existing(String),
    Generated(u128),
}
impl Default for HistoryGroup {
    fn default() -> Self {
        Self::Existing(String::new())
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ScheduledItem {
    Recipe(Box<Recipe>),
    Original {
        history_group: HistoryGroup,
        attempt: u32,
    },
}

/// Each request performs a concrete effect. No effect asks Go to decide whether
/// to retry, accept a save, reset metadata, enqueue or notify listeners.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Effect {
    UpdateMetadata(Config),
    Clock(ClockSite),
    Uuid(UuidSite),
    SavePending(Vec<String>),
    ActivateBranch {
        history_group: HistoryGroup,
    },
    PrepareSave {
        opcode: Opcode,
        history_group: HistoryGroup,
    },
    SaveStep {
        id: String,
        output: Vec<u8>,
    },
    LogExistingOutput(Fault),
    PrepareRetrySpan {
        opcode: Opcode,
        history_group: HistoryGroup,
    },
    Lifecycle {
        status: Status,
        attempt: u32,
        at: Time,
    },
    ReleaseCapacity,
    ResetMetadata(Config),
    PreparePlanned {
        recipe: Recipe,
        handled_at: Time,
    },
    PrepareDiscovery(Recipe),
    Enqueue(Recipe),
    FinishSpan {
        send: bool,
    },
    AsyncScheduled {
        item: ScheduledItem,
        name: Option<String>,
    },
    SyncScheduled {
        item: ScheduledItem,
        name: Option<String>,
        at: Time,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Owner {
    Response,
    Branch(u32),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Key {
    pub generation: u64,
    pub owner: Owner,
    pub sequence: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub key: Key,
    pub effect: Effect,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Observation {
    Ack(Option<Fault>),
    Done,
    BranchPanicked,
    Clock(Time),
    Uuid(u128),
    Prepared(Result<Vec<u8>, Fault>),
    Saved {
        has_pending: bool,
        error: Option<Fault>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Progress {
    /// Newly ready requests only; each key remains outstanding until resumed.
    pub ready: Vec<Request>,
    /// None while joining, Some(None) on success, Some(Some(..)) on failure.
    pub completion: Option<Option<Disposition>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Reject {
    InvalidInput,
    UnsupportedBatch,
    UnknownRequest,
    MismatchedResult,
}

#[derive(Clone, Copy, Debug)]
enum Await {
    Metadata,
    Handling,
    Pending,
    History,
    Activated,
    Prepared,
    Saved,
    ExistingLog,
    RetrySpan,
    LifecycleClock,
    Lifecycle,
    Release,
    RecipeUuid,
    RecipeClock,
    Reset,
    QueuePreparation,
    Enqueue,
    Span,
    Async,
    CallbackClock,
    Sync,
}
#[derive(Clone, Debug)]
struct Pending {
    request: Request,
    next: Await,
}
#[derive(Clone, Debug, Default)]
struct Branch {
    history: HistoryGroup,
    failed: bool,
    retrying: bool,
    has_pending: bool,
    recipe_group: Option<u128>,
    recipe: Option<Recipe>,
    error: Option<Disposition>,
    enqueue_duplicate: bool,
    finished: bool,
}

#[derive(Debug)]
pub struct Session {
    input: Input,
    branches: Vec<Branch>,
    pending: Vec<Option<Pending>>,
    sequences: Vec<u64>,
    handled_at: Time,
    coalesce_key: Option<String>,
    first_error: Option<Disposition>,
    completion: Option<Option<Disposition>>,
}

pub fn coalesce_key(run_id: &str, ids: &[String]) -> String {
    let mut sorted = ids.to_vec();
    sorted.sort(); // Keep duplicates and UTF-8 byte order, as Go sort.Strings.
    let mut hash = Xxh64::new(0);
    hash.update(run_id.as_bytes());
    for id in sorted {
        hash.update(&[0]);
        hash.update(id.as_bytes());
    }
    format!("{:016x}", hash.digest())
}

impl Session {
    pub fn start(input: Input) -> Result<(Self, Progress), Reject> {
        if input.generation == 0
            || input.ops.len() > MAX_OPCODES
            || input.edge.is_none()
            || input.attempt == u32::MAX
            || input.handled_step_error == 0
            || input.function_overflow == 0
        {
            return Err(Reject::InvalidInput);
        }
        // Whole-batch validation precedes even metadata. Nil/unsupported/error-
        // descriptor inputs remain eligible for a future whole-response Go path.
        for op in &input.ops {
            let Some(op) = op else {
                return Err(Reject::InvalidInput);
            };
            match OpcodeTag::try_from(op.tag) {
                Ok(
                    OpcodeTag::None
                    | OpcodeTag::Step
                    | OpcodeTag::StepRun
                    | OpcodeTag::StepFailed
                    | OpcodeTag::StepPlanned,
                ) => {}
                Ok(OpcodeTag::StepError) if op.error.is_some() => {}
                Ok(OpcodeTag::StepError) => return Err(Reject::InvalidInput),
                _ => return Err(Reject::UnsupportedBatch),
            }
        }
        let n = input.ops.len();
        let mut s = Self {
            branches: vec![Branch::default(); n],
            pending: vec![None; n + 1],
            sequences: vec![0; n + 1],
            input,
            handled_at: 0,
            coalesce_key: None,
            first_error: None,
            completion: None,
        };
        let mut ready = Vec::new();
        let config = &s.input.config;
        let ai = s.input.ops.iter().flatten().any(|op| op.has_ai);
        if config.request_version == -1 || (n > 1 && !config.force_step_plan) || ai {
            let proposal = Config {
                request_version: s.input.response_version,
                force_step_plan: config.force_step_plan || n > 1,
                started_at: config.started_at,
                // A proposal starts with HasAI false, rather than merging the
                // loaded flag. Reset proposals below do preserve loaded HasAI.
                has_ai: ai,
            };
            s.issue(
                n,
                Effect::UpdateMetadata(proposal),
                Await::Metadata,
                &mut ready,
            );
        } else {
            s.after_metadata(&mut ready);
        }
        let progress = s.progress(ready);
        Ok((s, progress))
    }

    /// Reject without consuming a key or changing state. Results may arrive in
    /// any sibling order; one branch can reach enqueue while another saves.
    pub fn resume(&mut self, key: Key, result: Observation) -> Result<Progress, Reject> {
        let slot = match key.owner {
            Owner::Response => self.branches.len(),
            Owner::Branch(i) => {
                let i = i as usize;
                if i >= self.branches.len() {
                    return Err(Reject::UnknownRequest);
                }
                i
            }
        };
        let Some(pending) = self.pending.get(slot).and_then(Option::as_ref) else {
            return Err(Reject::UnknownRequest);
        };
        if pending.request.key != key {
            return Err(Reject::UnknownRequest);
        }
        if result == Observation::BranchPanicked {
            if slot == self.branches.len()
                || matches!(pending.request.effect, Effect::Uuid(UuidSite::History))
            {
                return Err(Reject::MismatchedResult);
            }
            self.pending[slot] = None;
            self.finish(slot, None);
            return Ok(self.progress(Vec::new()));
        }
        if !compatible(&pending.request.effect, &result) {
            return Err(Reject::MismatchedResult);
        }
        let next = pending.next;
        self.pending[slot] = None;
        let mut ready = Vec::new();
        self.advance(slot, next, result, &mut ready);
        Ok(self.progress(ready))
    }

    pub fn outstanding(&self) -> impl Iterator<Item = &Request> {
        self.pending.iter().flatten().map(|p| &p.request)
    }

    pub fn original_attempt(&self) -> u32 {
        self.input.attempt
    }

    fn progress(&self, ready: Vec<Request>) -> Progress {
        Progress {
            ready,
            completion: self.completion.clone(),
        }
    }
    fn issue(&mut self, slot: usize, effect: Effect, next: Await, ready: &mut Vec<Request>) {
        debug_assert!(self.pending[slot].is_none());
        self.sequences[slot] += 1;
        let owner = if slot == self.branches.len() {
            Owner::Response
        } else {
            Owner::Branch(slot as u32)
        };
        let request = Request {
            key: Key {
                generation: self.input.generation,
                owner,
                sequence: self.sequences[slot],
            },
            effect,
        };
        self.pending[slot] = Some(Pending {
            request: request.clone(),
            next,
        });
        ready.push(request);
    }
    fn op(&self, i: usize) -> &Opcode {
        self.input.ops[i].as_ref().expect("validated opcode")
    }
    fn fail(&mut self, i: usize, fault: Fault, context: ErrorContext) {
        self.finish(
            i,
            Some(Disposition {
                fault,
                context,
                policy: RetryPolicy::Original,
            }),
        );
    }
    fn finish(&mut self, i: usize, error: Option<Disposition>) {
        self.branches[i].finished = true;
        if self.first_error.is_none() {
            self.first_error = error;
        }
        if self.branches.iter().all(|b| b.finished) {
            let mut error = self.first_error.clone();
            if let Some(e) = &mut error {
                // Discovery's Go alwaysRetry wrapper embeds error without
                // Unwrap: an underlying overflow is no longer errors.Is-visible.
                // Keep original classifications/identity on the retained handle.
                e.policy = if e.context == ErrorContext::Discovery {
                    RetryPolicy::Always
                } else if e.fault.state_overflow || e.fault.always_retry {
                    RetryPolicy::Original
                } else if self.input.response_no_retry {
                    RetryPolicy::Never
                } else if let Some(at) = self.input.retry_at {
                    RetryPolicy::At(at)
                } else {
                    RetryPolicy::Original
                };
            }
            self.completion = Some(error);
        }
    }
    fn terminal(&mut self, fault: Fault, context: ErrorContext) {
        self.completion = Some(Some(Disposition {
            fault,
            context,
            policy: RetryPolicy::Original,
        }));
    }
    fn after_metadata(&mut self, ready: &mut Vec<Request>) {
        let n = self.branches.len();
        if n > STEP_LIMIT {
            self.terminal(
                Fault {
                    handle: self.input.function_overflow,
                    ..Fault::default()
                },
                ErrorContext::Original,
            );
        } else {
            self.issue(
                n,
                Effect::Clock(ClockSite::Handling),
                Await::Handling,
                ready,
            );
        }
    }
    fn launch(&mut self, ready: &mut Vec<Request>) {
        if self.branches.is_empty() {
            self.completion = Some(None);
        }
        for i in 0..self.branches.len() {
            if self.branches.len() > 1 {
                self.issue(i, Effect::Uuid(UuidSite::History), Await::History, ready);
            } else {
                self.branches[i].history = HistoryGroup::Existing(self.input.context_group.clone());
                self.begin_branch(i, ready);
            }
        }
    }
    fn begin_branch(&mut self, i: usize, ready: &mut Vec<Request>) {
        self.issue(
            i,
            Effect::ActivateBranch {
                history_group: self.branches[i].history.clone(),
            },
            Await::Activated,
            ready,
        );
    }
    fn activated_branch(&mut self, i: usize, ready: &mut Vec<Request>) {
        let op = self.op(i);
        if op.tag == OpcodeTag::None as u8 {
            self.finish(i, None);
            return;
        }
        if op.tag == OpcodeTag::StepPlanned as u8 {
            self.issue(i, Effect::Uuid(UuidSite::Planned), Await::RecipeUuid, ready);
            return;
        }
        let retrying = op.tag == OpcodeTag::StepError as u8
            && !op.error.as_ref().expect("validated StepError").no_retry
            && !self.input.response_no_retry
            && !self.input.response_user_no_retry
            && self.input.attempt + 1 < self.input.max_attempts;
        self.branches[i].retrying = retrying;
        self.branches[i].failed = matches!(self.op(i).tag, 3 | 12);
        let mut opcode = self.op(i).clone();
        let history_group = self.branches[i].history.clone();
        if retrying {
            self.issue(
                i,
                Effect::PrepareRetrySpan {
                    opcode,
                    history_group,
                },
                Await::RetrySpan,
                ready,
            );
        } else {
            if self.branches[i].failed {
                opcode.tag = OpcodeTag::StepFailed as u8;
            }
            self.issue(
                i,
                Effect::PrepareSave {
                    opcode,
                    history_group,
                },
                Await::Prepared,
                ready,
            );
        }
    }
    fn scheduled(&self, i: usize) -> (ScheduledItem, Option<String>) {
        if self.branches[i].retrying {
            (
                ScheduledItem::Original {
                    history_group: self.branches[i].history.clone(),
                    attempt: self.input.attempt + 1,
                },
                Some(self.op(i).name.clone()),
            )
        } else {
            let name =
                (self.op(i).tag == OpcodeTag::StepPlanned as u8).then(|| self.op(i).name.clone());
            (
                ScheduledItem::Recipe(Box::new(
                    self.branches[i].recipe.clone().expect("recipe binding"),
                )),
                name,
            )
        }
    }
    fn callbacks(&mut self, i: usize, ready: &mut Vec<Request>) {
        let (item, name) = self.scheduled(i);
        self.issue(
            i,
            Effect::AsyncScheduled { item, name },
            Await::Async,
            ready,
        );
    }
    fn prepare_queue(&mut self, i: usize, ready: &mut Vec<Request>) {
        let recipe = self.branches[i].recipe.clone().expect("recipe binding");
        let effect = if self.op(i).tag == OpcodeTag::StepPlanned as u8 {
            Effect::PreparePlanned {
                recipe,
                handled_at: self.handled_at,
            }
        } else {
            Effect::PrepareDiscovery(recipe)
        };
        self.issue(i, effect, Await::QueuePreparation, ready);
    }
    fn recipe(&self, i: usize, group: u128, now: Time) -> Recipe {
        let op = self.op(i);
        let planned = op.tag == OpcodeTag::StepPlanned as u8;
        let failed = self.branches[i].failed;
        let mut bindings = self.input.queue.clone();
        if planned {
            bindings.step_semaphores.clear();
        }
        let job_id = if planned {
            format!("{}-{}-plan", self.input.idempotency_key, op.id)
        } else if failed {
            format!("{}-{}-failure", self.input.idempotency_key, op.id)
        } else if let Some(ck) = self
            .input
            .context_coalesce_key
            .as_ref()
            .filter(|k| !k.is_empty() && op.parallel_mode != ParallelMode::Race)
        {
            format!("{}-{}-discover", self.input.run_id, ck)
        } else {
            format!("{}-{}", self.input.idempotency_key, op.id)
        };
        let original = self.input.edge.as_ref().expect("validated edge");
        let edge = if planned {
            Edge {
                incoming_generator_step: op.id.clone(),
                incoming_generator_step_name: op.display_name.clone().unwrap_or(op.name.clone()),
                incoming: original.incoming.clone(),
                outgoing: original.outgoing.clone(),
            }
        } else {
            Edge {
                incoming: original.incoming.clone(),
                outgoing: op.id.clone(),
                ..Edge::default()
            }
        };
        Recipe {
            job_id,
            group,
            kind: if failed { Kind::EdgeError } else { Kind::Edge },
            edge,
            at: if planned { now - 1_000_000 } else { now },
            attempt: 0,
            max_attempts: self.input.max_attempts,
            parallel_mode: op.parallel_mode,
            parallel_coalesce_key: if planned {
                self.coalesce_key.clone()
            } else {
                None
            },
            bindings,
            passthrough_job_id: false,
        }
    }
    fn advance(&mut self, i: usize, next: Await, result: Observation, ready: &mut Vec<Request>) {
        match (next, result) {
            (Await::Metadata, Observation::Ack(error)) => {
                if let Some(e) = error {
                    self.terminal(e, ErrorContext::Metadata);
                } else {
                    self.after_metadata(ready);
                }
            }
            (Await::Handling, Observation::Clock(now)) => {
                self.handled_at = now;
                let version = if self.input.config.request_version == -1 {
                    self.input.response_version
                } else {
                    self.input.config.request_version
                };
                if self.branches.len() > 1
                    && version >= 2
                    && self
                        .input
                        .ops
                        .iter()
                        .flatten()
                        .any(|op| op.tag == OpcodeTag::StepPlanned as u8)
                {
                    let ids = self
                        .input
                        .ops
                        .iter()
                        .flatten()
                        .map(|op| op.id.clone())
                        .collect::<Vec<_>>();
                    self.coalesce_key = Some(coalesce_key(&self.input.run_id, &ids));
                    self.issue(i, Effect::SavePending(ids), Await::Pending, ready);
                } else {
                    self.launch(ready);
                }
            }
            (Await::Pending, Observation::Ack(error)) => {
                if let Some(e) = error {
                    self.terminal(e, ErrorContext::Pending);
                } else {
                    self.launch(ready);
                }
            }
            (Await::History, Observation::Uuid(uuid)) => {
                self.branches[i].history = HistoryGroup::Generated(uuid);
                self.begin_branch(i, ready);
            }
            (Await::Activated, Observation::Done) => self.activated_branch(i, ready),
            (Await::Prepared, Observation::Prepared(result)) => match result {
                Ok(output) => self.issue(
                    i,
                    Effect::SaveStep {
                        id: self.op(i).id.clone(),
                        output,
                    },
                    Await::Saved,
                    ready,
                ),
                Err(e) => self.fail(i, e, ErrorContext::Original),
            },
            (Await::Saved, Observation::Saved { has_pending, error }) => {
                if let Some(e) = &error
                    && !e.idempotent
                    && (self.branches[i].failed || !e.duplicate)
                {
                    self.fail(i, e.clone(), ErrorContext::Original);
                    return;
                }
                self.branches[i].has_pending = has_pending;
                if !self.branches[i].failed
                    && let Some(e) = error
                {
                    self.issue(i, Effect::LogExistingOutput(e), Await::ExistingLog, ready);
                } else {
                    self.issue(
                        i,
                        Effect::Clock(ClockSite::Lifecycle),
                        Await::LifecycleClock,
                        ready,
                    );
                }
            }
            (Await::ExistingLog, Observation::Done) => self.issue(
                i,
                Effect::Clock(ClockSite::Lifecycle),
                Await::LifecycleClock,
                ready,
            ),
            (Await::RetrySpan, Observation::Done) => self.issue(
                i,
                Effect::Clock(ClockSite::Lifecycle),
                Await::LifecycleClock,
                ready,
            ),
            (Await::LifecycleClock, Observation::Clock(at)) => {
                let b = &self.branches[i];
                let status = if b.retrying {
                    Status::Errored
                } else if b.failed {
                    Status::Failed
                } else {
                    Status::Finished
                };
                self.issue(
                    i,
                    Effect::Lifecycle {
                        status,
                        attempt: self.input.attempt,
                        at,
                    },
                    Await::Lifecycle,
                    ready,
                );
            }
            (Await::Lifecycle, Observation::Done) => {
                if self.branches[i].retrying {
                    self.callbacks(i, ready);
                } else {
                    self.issue(i, Effect::ReleaseCapacity, Await::Release, ready);
                }
            }
            (Await::Release, Observation::Done) => self.issue(
                i,
                Effect::Uuid(UuidSite::Discovery),
                Await::RecipeUuid,
                ready,
            ),
            (Await::RecipeUuid, Observation::Uuid(uuid)) => {
                // Store UUID until the later recipe clock is observed. No clock
                // is sampled early merely because the future recipe needs one.
                self.branches[i].recipe_group = Some(uuid);
                self.issue(
                    i,
                    Effect::Clock(ClockSite::Recipe),
                    Await::RecipeClock,
                    ready,
                );
            }
            (Await::RecipeClock, Observation::Clock(now)) => {
                let group = self.branches[i].recipe_group.expect("uuid binding");
                self.branches[i].recipe = Some(self.recipe(i, group, now));
                let planned = self.op(i).tag == OpcodeTag::StepPlanned as u8;
                let mode = if self.branches[i].failed {
                    self.input.context_mode
                } else {
                    self.op(i).parallel_mode
                };
                if !planned && self.branches[i].has_pending && mode != ParallelMode::Race {
                    self.callbacks(i, ready);
                } else if !planned
                    && self.input.config.request_version >= 2
                    && self.input.config.force_step_plan
                {
                    let mut reset = self.input.config.clone();
                    reset.force_step_plan = false;
                    self.issue(i, Effect::ResetMetadata(reset), Await::Reset, ready);
                } else {
                    self.prepare_queue(i, ready);
                }
            }
            (Await::Reset, Observation::Ack(error)) => {
                if let Some(e) = error {
                    self.fail(i, e, ErrorContext::Reset);
                } else {
                    self.prepare_queue(i, ready);
                }
            }
            (Await::QueuePreparation, Observation::Done) => {
                let recipe = self.branches[i].recipe.clone().expect("recipe binding");
                self.issue(i, Effect::Enqueue(recipe), Await::Enqueue, ready);
            }
            (Await::Enqueue, Observation::Ack(error)) => {
                let duplicate = error.as_ref().is_some_and(|e| e.duplicate);
                let planned = self.op(i).tag == OpcodeTag::StepPlanned as u8;
                let send = !duplicate && (planned || error.is_none());
                self.branches[i].error = error.filter(|_| !duplicate).map(|fault| Disposition {
                    fault,
                    context: if planned {
                        ErrorContext::Original
                    } else {
                        ErrorContext::Discovery
                    },
                    policy: RetryPolicy::Original,
                });
                // Duplicate returns nil after Drop; mark callback suppression
                // separately from a failed enqueue that must retain its handle.
                self.branches[i].enqueue_duplicate = duplicate;
                self.issue(i, Effect::FinishSpan { send }, Await::Span, ready);
            }
            (Await::Span, Observation::Done) => {
                let b = &self.branches[i];
                if b.enqueue_duplicate {
                    self.finish(i, None);
                } else if b.error.is_some() && self.op(i).tag != OpcodeTag::StepPlanned as u8 {
                    let error = self.branches[i].error.take();
                    self.finish(i, error);
                } else {
                    self.callbacks(i, ready);
                }
            }
            (Await::Async, Observation::Done) => self.issue(
                i,
                Effect::Clock(ClockSite::Callback),
                Await::CallbackClock,
                ready,
            ),
            (Await::CallbackClock, Observation::Clock(at)) => {
                let (item, name) = self.scheduled(i);
                self.issue(
                    i,
                    Effect::SyncScheduled { item, name, at },
                    Await::Sync,
                    ready,
                );
            }
            (Await::Sync, Observation::Done) => {
                let error = if self.branches[i].retrying {
                    Some(Disposition {
                        fault: Fault {
                            handle: self.input.handled_step_error,
                            ..Fault::default()
                        },
                        context: ErrorContext::Original,
                        policy: RetryPolicy::Original,
                    })
                } else {
                    self.branches[i].error.take()
                };
                self.finish(i, error);
            }
            _ => unreachable!("validated observation kind"),
        }
    }
}

fn compatible(effect: &Effect, observation: &Observation) -> bool {
    match (effect, observation) {
        (
            Effect::UpdateMetadata(_)
            | Effect::SavePending(_)
            | Effect::ResetMetadata(_)
            | Effect::Enqueue(_),
            Observation::Ack(error),
        ) => error.as_ref().is_none_or(|e| e.handle != 0),
        (Effect::Clock(_), Observation::Clock(at)) => at.checked_sub(1_000_000).is_some(),
        (Effect::Uuid(_), Observation::Uuid(id)) => *id != 0,
        (Effect::PrepareSave { .. }, Observation::Prepared(result)) => {
            result.as_ref().err().is_none_or(|e| e.handle != 0)
        }
        (Effect::SaveStep { .. }, Observation::Saved { error, .. }) => {
            error.as_ref().is_none_or(|e| e.handle != 0)
        }
        (
            Effect::ActivateBranch { .. }
            | Effect::PrepareRetrySpan { .. }
            | Effect::LogExistingOutput(_)
            | Effect::Lifecycle { .. }
            | Effect::ReleaseCapacity
            | Effect::PreparePlanned { .. }
            | Effect::PrepareDiscovery(_)
            | Effect::FinishSpan { .. }
            | Effect::AsyncScheduled { .. }
            | Effect::SyncScheduled { .. },
            Observation::Done,
        ) => true,
        _ => false,
    }
}
