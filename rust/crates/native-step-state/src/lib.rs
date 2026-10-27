//! Internal, synchronous native-owner experiment. No discovery, worker, Go
//! callbacks or production selector. See the task's native-save reference.
mod json;
mod runtime;
pub use runtime::{CallContext, ErrorClass, Runtime, SaveOutcome, StoreConfig, WriteStatus};
use serde::{Deserialize, Serialize};

pub const SAVE_SHA1: &str = "2c371556f31bb03e150253bbd588a9a34255061a";
pub const SAVE_LUA: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/saveResponse.lua"));
pub const MAX_STEP_OUTPUT: usize = 4 * 1024 * 1024;
pub const DEFAULT_MAX_STATE: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparationError {
    InvalidJson,
    Ineligible,
    ErrStepOutputTooLarge,
    ErrStateOverflowed { limit: usize },
    InvalidConfiguration,
}

impl PreparationError {
    pub fn class(&self) -> &'static str {
        match self {
            Self::InvalidJson => "InvalidJson",
            Self::Ineligible => "Ineligible",
            Self::ErrStepOutputTooLarge => "ErrStepOutputTooLarge",
            Self::ErrStateOverflowed { .. } => "ErrStateOverflowed",
            Self::InvalidConfiguration => "InvalidConfiguration",
        }
    }
    /// Go's observable standard-error value, without claiming sentinel identity
    /// or reconstructing a Go error chain across the process boundary.
    pub fn standard_error(&self) -> Option<StandardError> {
        let Self::ErrStateOverflowed { limit } = self else {
            return None;
        };
        let name = "InngestErrStateOverflowed";
        let message = format!("The function run exceeded the state size limit of {limit} bytes.");
        Some(StandardError {
            name,
            error: format!("{name}: {message}"),
            message,
        })
    }
}
impl Serialize for PreparationError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.class())
    }
}
#[derive(Debug, Serialize)]
pub struct StandardError {
    pub error: String,
    pub name: &'static str,
    pub message: String,
}
impl std::fmt::Display for PreparationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(standard) = self.standard_error() {
            return f.write_str(&standard.error);
        }
        f.write_str(match self {
            Self::ErrStepOutputTooLarge => "step output size is greater than the limit",
            _ => self.class(),
        })
    }
}
impl std::error::Error for PreparationError {}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunIdentity {
    pub run_id: String,
    pub function_id: String,
    pub account_id: String,
    pub env_id: String,
    pub app_id: String,
}
impl RunIdentity {
    fn valid(&self) -> bool {
        fn uuid(s: &str) -> bool {
            s.len() == 36
                && s.bytes().enumerate().all(|(i, c)| {
                    if [8, 13, 18, 23].contains(&i) {
                        c == b'-'
                    } else {
                        c.is_ascii_digit() || (b'a'..=b'f').contains(&c)
                    }
                })
        }
        self.run_id.len() == 26
            && self.run_id.as_bytes()[0] <= b'7'
            && self
                .run_id
                .bytes()
                .all(|c| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&c))
            && [
                &self.function_id,
                &self.account_id,
                &self.env_id,
                &self.app_id,
            ]
            .into_iter()
            .all(|s| uuid(s))
    }
}
#[derive(Clone, Copy, Deserialize)]
pub enum Layout {
    AlwaysShard,
    NeverShard,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StateConfig {
    pub prefix: String,
    pub layout: Layout,
    pub identity: RunIdentity,
}
impl StateConfig {
    pub fn keys(&self) -> Result<[String; 5], PreparationError> {
        if self.prefix.is_empty() || self.prefix.contains(['{', '}']) || !self.identity.valid() {
            return Err(PreparationError::InvalidConfiguration);
        }
        let run = &self.identity.run_id;
        let function = &self.identity.function_id;
        let tag = match self.layout {
            Layout::AlwaysShard => format!("{{{}:{run}}}", self.prefix),
            Layout::NeverShard => format!("{{{}}}", self.prefix),
        };
        Ok([
            format!("{tag}:actions:{function}:{run}"),
            format!("{tag}:metadata:{run}"),
            format!("{tag}:stack:{run}"),
            format!("{tag}:inputs:{function}:{run}"),
            format!("{tag}:pending:{function}:{run}"),
        ])
    }
}

/// Explicit host admission, not a claim that native tracing or AI metadata is
/// implemented. Only isolated runs whose trace owner is disabled are eligible.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PreparationConfig {
    pub request_version: u8,
    pub force_step_plan: bool,
    pub trace_disabled: bool,
    pub has_ai: bool,
    pub sdk_metadata: Vec<serde_json::Value>,
    pub experiments: Vec<serde_json::Value>,
    pub loaded_state_bytes: usize,
    /// None mirrors an absent Go stateSizeLimit callback; Some(0) uses default.
    pub state_limit: Option<usize>,
}

pub struct PreparedStep {
    step_id: String,
    output: Vec<u8>,
}
impl PreparedStep {
    pub fn step_id(&self) -> &str {
        &self.step_id
    }
    pub fn output(&self) -> &[u8] {
        &self.output
    }
}

pub fn prepare_response(
    raw: &[u8],
    config: &PreparationConfig,
) -> Result<PreparedStep, PreparationError> {
    if config.request_version != 2
        || config.force_step_plan
        || !config.trace_disabled
        || config.has_ai
        || !config.sdk_metadata.is_empty()
        || !config.experiments.is_empty()
    {
        return Err(PreparationError::Ineligible);
    }
    let (step_id, data) = json::response(raw)?;
    let compact = match data {
        Some(d) => json::compact(d)?,
        None => b"null".to_vec(),
    };
    // This conservative gate excludes every payload on which the current Go
    // AI extractor might run. No attempt to silently omit generated metadata.
    if compact.windows(10).any(|w| w == b"totalUsage")
        || compact.windows(11).any(|w| w == b"inputTokens")
    {
        return Err(PreparationError::Ineligible);
    }
    let mut output = b"{\"data\":".to_vec();
    output.extend_from_slice(&compact);
    output.push(b'}');
    if output.len() > MAX_STEP_OUTPUT {
        return Err(PreparationError::ErrStepOutputTooLarge);
    }
    if let Some(limit) = config.state_limit {
        let limit = if limit == 0 { DEFAULT_MAX_STATE } else { limit };
        if config
            .loaded_state_bytes
            .checked_add(output.len())
            .is_none_or(|size| size > limit)
        {
            return Err(PreparationError::ErrStateOverflowed { limit });
        }
    }
    Ok(PreparedStep { step_id, output })
}

/// SaveStep itself accepts opaque bytes, just like Go. Preparation is a distinct
/// owner. A positive adapter delta is supplied explicitly, never generated here.
pub struct SaveRequest {
    keys: [String; 5],
    step_id: Vec<u8>,
    output: Vec<u8>,
    metadata_delta: i64,
}
impl SaveRequest {
    pub fn new(
        config: &StateConfig,
        step_id: Vec<u8>,
        output: Vec<u8>,
        metadata_delta: i64,
    ) -> Result<Self, PreparationError> {
        Ok(Self {
            keys: config.keys()?,
            step_id,
            output,
            metadata_delta,
        })
    }
    pub fn prepared(config: &StateConfig, step: PreparedStep) -> Result<Self, PreparationError> {
        Self::new(config, step.step_id.into_bytes(), step.output, 0)
    }
}
