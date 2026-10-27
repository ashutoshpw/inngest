use crate::{
    ENQUEUE_LUA, ENQUEUE_SHA1, EnqueueRequest, PreparationError, SAVE_LUA, SAVE_SHA1, SaveRequest,
};
use redis::IntoConnectionInfo;
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

// Deliberately no Debug: credentials must not enter diagnostics or helper output.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StoreConfig {
    /// Literal IP:port only: avoid unbounded DNS and implicit address failover.
    pub address: SocketAddr,
    pub username: Option<String>,
    pub password: String,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub write_timeout_ms: u64,
}

/// Cooperative cancellation between blocking I/O phases and during backoff.
/// A blocking redis-rs operation is bounded by socket timeouts, not preempted by
/// cancellation. Those timeouts are inactivity limits, not a dribble-proof total
/// deadline. No stronger guarantee than this is exposed or claimed.
pub struct CallContext {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
}
impl CallContext {
    pub fn new(budget: Duration) -> Self {
        Self {
            deadline: Instant::now() + budget,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn cancellation(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }
    fn remaining(&self) -> Option<Duration> {
        if self.cancelled.load(Ordering::Acquire) {
            None
        } else {
            self.deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
        }
    }
    fn timeout(&self, configured_ms: u64) -> Option<Duration> {
        self.remaining()
            .map(|left| left.min(Duration::from_millis(configured_ms)))
    }
    fn wait(&self, delay: Duration) -> bool {
        let until = Instant::now() + delay;
        while let Some(left) = until.checked_duration_since(Instant::now()) {
            let Some(budget) = self.remaining() else {
                return false;
            };
            std::thread::sleep(left.min(budget).min(Duration::from_millis(10)));
        }
        self.remaining().is_some()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ErrorClass {
    None,
    ErrIdempotentResponse,
    ErrDuplicateResponse,
    ErrMaxAttemptReached,
    Stopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum WriteStatus {
    /// No potentially effective save was issued (a NOSCRIPT lookup may occur).
    NotSent,
    /// A script reply proves this request did not change an existing output.
    NotApplied,
    /// A sent request failed without a definitive reply; partial Lua errors also
    /// belong here. This is not a statement that a write actually happened.
    MayHaveCommitted,
    /// Requested output bytes are known present, including an idempotent reply.
    /// This does not prove that this invocation made a new write.
    Confirmed,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveOutcome {
    pub has_pending: bool,
    pub error: ErrorClass,
    pub attempts: usize,
    /// Only replies actually received by the native caller; lost replies are
    /// observed independently by the relay/Go reader, not invented here.
    pub results: Vec<Vec<i64>>,
    pub write_status: WriteStatus,
    pub last_failure: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum EnqueueClass {
    Enqueued,
    Duplicate,
    ExcludedSingletonConflict,
    Failed,
    Stopped,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnqueueOutcome {
    pub class: EnqueueClass,
    pub stored_id: String,
    pub existing_run_id: Option<String>,
    pub result: Option<i64>,
    pub write_status: WriteStatus,
    pub last_failure: Option<&'static str>,
}

pub struct Runtime {
    config: StoreConfig,
    client: redis::Client,
    connection: Option<redis::Connection>,
}
struct Failure {
    status: WriteStatus,
    class: &'static str,
}
impl Runtime {
    pub fn new(config: StoreConfig) -> Result<Self, PreparationError> {
        if config.password.is_empty()
            || [
                config.connect_timeout_ms,
                config.read_timeout_ms,
                config.write_timeout_ms,
            ]
            .contains(&0)
        {
            return Err(PreparationError::InvalidConfiguration);
        }
        let mut settings = redis::RedisConnectionInfo::default()
            .set_password(&config.password)
            .set_protocol(redis::ProtocolVersion::RESP2)
            .set_skip_set_lib_name();
        if let Some(username) = &config.username {
            settings = settings.set_username(username);
        }
        let info = (config.address.ip().to_string(), config.address.port())
            .into_connection_info()
            .map_err(|_| PreparationError::InvalidConfiguration)?
            .set_redis_settings(settings);
        let client =
            redis::Client::open(info).map_err(|_| PreparationError::InvalidConfiguration)?;
        Ok(Self {
            config,
            client,
            connection: None,
        })
    }

    fn command<T: redis::FromRedisValue>(
        &mut self,
        ctx: &CallContext,
        cmd: &redis::Cmd,
    ) -> Result<T, Failure> {
        let stopped = || Failure {
            status: WriteStatus::NotSent,
            class: "stopped",
        };
        if self.connection.is_none() {
            let timeout = ctx
                .timeout(self.config.connect_timeout_ms)
                .ok_or_else(stopped)?;
            self.connection = Some(self.client.get_connection_with_timeout(timeout).map_err(
                |_| Failure {
                    status: WriteStatus::NotSent,
                    class: "connect-or-auth",
                },
            )?);
        }
        let read = ctx
            .timeout(self.config.read_timeout_ms)
            .ok_or_else(stopped)?;
        let write = ctx
            .timeout(self.config.write_timeout_ms)
            .ok_or_else(stopped)?;
        let connection = self.connection.as_mut().unwrap();
        connection
            .set_read_timeout(Some(read))
            .map_err(|_| stopped())?;
        connection
            .set_write_timeout(Some(write))
            .map_err(|_| stopped())?;
        // Exactly one request. redis::Connection has no reconnection/write retry.
        let reply = cmd.query::<T>(connection);
        match reply {
            Ok(value) => Ok(value),
            Err(e) => {
                // Never reuse a socket with an unread/delayed response. Only the
                // adapter may open a new connection on its next attempt.
                if e.code() == Some("NOSCRIPT") {
                    Err(Failure {
                        status: WriteStatus::NotSent,
                        class: "NOSCRIPT",
                    })
                } else {
                    self.connection = None;
                    Err(Failure {
                        status: WriteStatus::MayHaveCommitted,
                        class: if e.is_timeout() {
                            "io-timeout"
                        } else if e.is_io_error() {
                            "io"
                        } else {
                            "server-or-protocol"
                        },
                    })
                }
            }
        }
    }

    /// Single enqueue attempt. Only an acknowledged NOSCRIPT permits exact
    /// EVAL fallback; any possibly sent failure returns without replaying it.
    /// SaveStep's separate Go-compatible retry heuristic is never used here.
    pub fn enqueue(&mut self, ctx: &CallContext, request: &EnqueueRequest) -> EnqueueOutcome {
        let mut out = EnqueueOutcome {
            class: EnqueueClass::Failed,
            stored_id: request.stored_id.clone(),
            existing_run_id: None,
            result: None,
            write_status: WriteStatus::NotSent,
            last_failure: None,
        };
        let command = |name: &str, script: &[u8]| {
            let mut c = redis::cmd(name);
            c.arg(script).arg(request.keys.len()).arg(&request.keys);
            for arg in &request.args {
                c.arg(arg);
            }
            c
        };
        let result = match self.command::<i64>(ctx, &command("EVALSHA", ENQUEUE_SHA1.as_bytes())) {
            Err(f) if f.class == "NOSCRIPT" => self.command(ctx, &command("EVAL", ENQUEUE_LUA)),
            result => result,
        };
        match result {
            Err(f) => {
                out.write_status = f.status;
                out.last_failure = Some(f.class);
                if f.class == "stopped" {
                    out.class = EnqueueClass::Stopped;
                }
            }
            Ok(code) => {
                out.result = Some(code);
                match code {
                    0 => {
                        out.class = EnqueueClass::Enqueued;
                        out.write_status = WriteStatus::Confirmed;
                    }
                    1 => {
                        out.write_status = WriteStatus::NotApplied;
                        // Go loads the current item after duplicate, including a
                        // different run sharing the same logical idempotency key.
                        let mut load = redis::cmd("HGET");
                        load.arg(&request.keys[0]).arg(&request.stored_id);
                        match self.command::<Option<Vec<u8>>>(ctx, &load) {
                            Ok(None) => out.class = EnqueueClass::Duplicate,
                            Ok(Some(raw)) => match crate::enqueue::existing_run_id(&raw) {
                                Some(run) => {
                                    out.existing_run_id = Some(run);
                                    out.class = EnqueueClass::Duplicate;
                                }
                                _ => out.last_failure = Some("duplicate-item-decode"),
                            },
                            Err(f) => out.last_failure = Some(f.class),
                        }
                    }
                    2 => {
                        out.class = EnqueueClass::ExcludedSingletonConflict;
                        out.write_status = WriteStatus::NotApplied;
                    }
                    _ => {
                        out.write_status = WriteStatus::MayHaveCommitted;
                        out.last_failure = Some("unexpected-script-result");
                    }
                }
            }
        }
        out
    }

    fn attempt(&mut self, ctx: &CallContext, request: &SaveRequest) -> Result<Vec<i64>, Failure> {
        let command = |name: &str, script: &[u8]| {
            let mut c = redis::cmd(name);
            c.arg(script)
                .arg(request.keys.len())
                .arg(&request.keys)
                .arg(&request.step_id)
                .arg(&request.output)
                .arg(request.metadata_delta);
            c
        };
        match self.command(ctx, &command("EVALSHA", SAVE_SHA1.as_bytes())) {
            Err(f) if f.class == "NOSCRIPT" => self.command(ctx, &command("EVAL", SAVE_LUA)),
            result => result,
        }
    }

    pub fn save_step(
        &mut self,
        ctx: &CallContext,
        request: &SaveRequest,
        disabled_retries: bool,
    ) -> SaveOutcome {
        let max_attempts = if disabled_retries { 1 } else { 10 };
        let mut out = SaveOutcome {
            has_pending: false,
            error: ErrorClass::None,
            attempts: 0,
            results: vec![],
            write_status: WriteStatus::NotSent,
            last_failure: None,
        };
        let mut backoff = Duration::from_millis(100);
        for attempt in 1..=max_attempts {
            if ctx.remaining().is_none() {
                out.error = ErrorClass::Stopped;
                return out;
            }
            out.attempts = attempt;
            match self.attempt(ctx, request) {
                Ok(result) => {
                    let code = result.first().copied();
                    out.has_pending = match code {
                        Some(1) => true,
                        Some(-2) => result.get(1) == Some(&1),
                        _ => false,
                    };
                    out.results.push(result);
                    match code {
                        Some(0 | 1) => {
                            out.write_status = WriteStatus::Confirmed;
                            return out;
                        }
                        Some(-2) => {
                            out.error = if attempt > 1 {
                                ErrorClass::None
                            } else {
                                ErrorClass::ErrIdempotentResponse
                            };
                            out.write_status = WriteStatus::Confirmed;
                            return out;
                        }
                        Some(-1) => {
                            if out.write_status == WriteStatus::NotSent {
                                out.write_status = WriteStatus::NotApplied;
                            }
                            // Preserve Go's heuristic. This does NOT prove an
                            // earlier attempt committed the requested output.
                            out.error = if attempt > 1 {
                                ErrorClass::None
                            } else {
                                ErrorClass::ErrDuplicateResponse
                            };
                            return out;
                        }
                        _ => {
                            out.write_status = WriteStatus::MayHaveCommitted;
                            out.last_failure = Some("unexpected-script-result");
                        }
                    }
                }
                Err(failure) => {
                    if failure.status == WriteStatus::MayHaveCommitted {
                        out.write_status = failure.status;
                    }
                    out.last_failure = Some(failure.class);
                }
            }
            if attempt == max_attempts {
                out.error = ErrorClass::ErrMaxAttemptReached;
                return out;
            }
            if !ctx.wait(backoff) {
                out.error = ErrorClass::Stopped;
                return out;
            }
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
        unreachable!()
    }
}
