use crate::{PreparationError, PreparedStep, StateConfig, json};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Deserialize)]
pub enum QueueMode {
    Partition,
    Key,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueueConfig {
    pub prefix: String,
    pub mode: QueueMode,
}

/// Only ordinary single-StepRun discovery is representable. Unknown descriptors
/// (parallel, constraints, routing, normalization, leases, etc.) fail decoding.
/// Trace/carrier generation remains the host's responsibility.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiscoveryConfig {
    pub queue: QueueConfig,
    pub save_completed: bool,
    pub has_pending: bool,
    pub idempotency_key: String,
    pub incoming: String,
    pub group_id: String,
    pub carrier: Option<String>,
    pub now_ms: i64,
    pub priority_factor: Option<i64>,
    pub identifier_priority_factor: Option<i64>,
    pub max_attempts: Option<u32>,
    pub function_version: u32,
    pub event_id: String,
    pub event_ids: Vec<String>,
}

pub struct EnqueueRequest {
    pub(crate) keys: Vec<String>,
    pub(crate) args: Vec<Vec<u8>>,
    pub(crate) stored_id: String,
}

pub(crate) fn ulid_time(value: &str) -> Option<i64> {
    if value.len() != 26 || value.as_bytes()[0] > b'7' {
        return None;
    }
    let mut timestamp = 0_i64;
    for (i, c) in value.bytes().enumerate() {
        let digit = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ"
            .iter()
            .position(|v| *v == c)?;
        if i < 10 {
            timestamp = timestamp * 32 + digit as i64;
        }
    }
    Some(timestamp)
}
fn hash_id(logical: &str) -> String {
    let mut value = xxhash_rust::xxh64::xxh64(logical.as_bytes(), 0);
    let mut digits = Vec::new();
    loop {
        digits.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(value % 36) as usize]);
        value /= 36;
        if value == 0 {
            break;
        }
    }
    digits.reverse();
    String::from_utf8(digits).unwrap()
}
fn encode(value: &impl Serialize) -> Vec<u8> {
    // Go's encoding/json also HTML-escapes string fields. No response data is
    // decoded here; its separate preparation owner preserves raw numeric bytes.
    json::compact(&serde_json::to_vec(value).unwrap()).unwrap()
}

// Declaration order follows the existing Go persistence contract.
#[derive(Serialize)]
struct Identifier<'a> {
    #[serde(rename = "runID")]
    run: &'a str,
    #[serde(rename = "wID")]
    function: &'a str,
    wv: u32,
    #[serde(rename = "evtID")]
    event: &'a str,
    #[serde(rename = "eventIDs")]
    events: &'a [String],
    #[serde(rename = "aID")]
    account: &'a str,
    #[serde(rename = "wsID")]
    env: &'a str,
    #[serde(rename = "appID")]
    app: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pf: Option<i64>,
}
#[derive(Serialize)]
struct Edge<'a> {
    incoming: &'a str,
    outgoing: &'a str,
}
#[derive(Serialize)]
struct Payload<'a> {
    edge: Edge<'a>,
}
#[derive(Serialize)]
struct Item<'a> {
    #[serde(rename = "groupID", skip_serializing_if = "str::is_empty")]
    group: &'a str,
    #[serde(rename = "wsID")]
    env: &'a str,
    kind: &'static str,
    identifier: Identifier<'a>,
    atts: u32,
    #[serde(rename = "maxAtts", skip_serializing_if = "Option::is_none")]
    max: Option<u32>,
    payload: Payload<'a>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<&'static str, &'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pf: Option<i64>,
}
#[derive(Serialize)]
struct QueueItem<'a> {
    id: &'a str,
    at: i64,
    wt: i64,
    #[serde(rename = "wfID")]
    function: &'a str,
    #[serde(rename = "wsID")]
    env: &'a str,
    #[serde(rename = "genID")]
    generation: u32,
    data: Item<'a>,
    eat: i64,
}
#[derive(Serialize)]
struct Partition<'a> {
    id: &'a str,
    wid: &'a str,
    #[serde(rename = "aID")]
    account: &'a str,
    last: i64,
    #[serde(rename = "forceAtMS")]
    force: i64,
}
#[derive(Serialize)]
struct Shadow<'a> {
    id: &'a str,
    fv: u32,
    fid: &'a str,
    eid: &'a str,
    aid: &'a str,
}
#[derive(Serialize)]
struct Backlog<'a> {
    id: &'a str,
    sid: &'a str,
    #[serde(skip_serializing_if = "is_zero")]
    fv: u32,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl EnqueueRequest {
    /// The caller must have completed native save first and supply its
    /// authoritative pending result. This is not a state reservation or an
    /// atomic save+enqueue operation. Preparation/admission precedes all I/O.
    pub fn new(
        state: &StateConfig,
        step: &PreparedStep,
        d: &DiscoveryConfig,
    ) -> Result<Self, PreparationError> {
        state.keys()?;
        if !d.save_completed || d.has_pending || d.event_ids.len() > 1 {
            return Err(PreparationError::Ineligible);
        }
        if d.queue.prefix.is_empty()
            || d.queue.prefix.contains(['{', '}'])
            || d.incoming.is_empty()
            || d.now_ms <= 0
            || ulid_time(&d.event_id).is_none()
            || d.event_ids.is_empty()
            || d.event_ids.iter().any(|id| ulid_time(id).is_none())
            || d.event_ids[0] != d.event_id
            || state.identity.function_id == "00000000-0000-0000-0000-000000000000"
        {
            return Err(PreparationError::InvalidConfiguration);
        }
        let id = &state.identity;
        let priority = d
            .priority_factor
            .or(d.identifier_priority_factor)
            .unwrap_or(0)
            .checked_mul(1000)
            .ok_or(PreparationError::InvalidConfiguration)?;
        let at = d
            .now_ms
            .checked_sub(priority)
            .ok_or(PreparationError::InvalidConfiguration)?;
        let start = ulid_time(&id.run_id).ok_or(PreparationError::InvalidConfiguration)?;
        let score = if start == 0 {
            d.now_ms
        } else {
            start
                .checked_sub(priority)
                .ok_or(PreparationError::InvalidConfiguration)?
        };
        // Bound to Lua's exact integer domain; this experiment does not accept
        // overflow/float-rounded scores or clocks before the run starts.
        if start > d.now_ms
            || at == 0
            || [at, score, d.now_ms]
                .iter()
                .any(|v| v.unsigned_abs() > (1_u64 << 53) - 1)
        {
            return Err(PreparationError::InvalidConfiguration);
        }
        let key = if d.idempotency_key.is_empty() {
            &id.run_id
        } else {
            &d.idempotency_key
        };
        let stored_id = hash_id(&format!("{}:{key}-{}", id.function_id, step.step_id));
        let mut metadata = BTreeMap::new();
        if let Some(carrier) = &d.carrier {
            metadata.insert("user-otel-ctx", carrier.as_str());
        }
        let item = encode(&QueueItem {
            id: &stored_id,
            at,
            wt: d.now_ms,
            function: &id.function_id,
            env: &id.env_id,
            generation: 1,
            eat: d.now_ms,
            data: Item {
                group: &d.group_id,
                env: &id.env_id,
                kind: "edge",
                atts: 0,
                max: d.max_attempts,
                identifier: Identifier {
                    run: &id.run_id,
                    function: &id.function_id,
                    wv: d.function_version,
                    event: &d.event_id,
                    events: &d.event_ids,
                    account: &id.account_id,
                    env: &id.env_id,
                    app: &id.app_id,
                    pf: d.identifier_priority_factor,
                },
                payload: Payload {
                    edge: Edge {
                        incoming: &d.incoming,
                        outgoing: &step.step_id,
                    },
                },
                metadata,
                pf: d.priority_factor,
            },
        });
        let partition = encode(&Partition {
            id: &id.function_id,
            wid: &id.function_id,
            account: &id.account_id,
            last: 0,
            force: 0,
        });
        let key_mode = matches!(d.queue.mode, QueueMode::Key);
        let backlog_id = if key_mode {
            format!("fn:{}", id.function_id)
        } else {
            String::new()
        };
        let shadow_id = if key_mode {
            id.function_id.as_str()
        } else {
            ""
        };
        let backlog_key = if key_mode { backlog_id.as_str() } else { "-" };
        let normalize = if key_mode {
            format!("partition:{}:sorted", id.function_id)
        } else {
            "-".into()
        };
        let suffixes = [
            "queue:item".into(),
            "partition:item".into(),
            "partition:sorted".into(),
            "accounts:sorted".into(),
            format!("accounts:{}:partition:sorted", id.account_id),
            format!("queue:seen:{stored_id}"),
            format!("queue:sorted:{}", id.function_id),
            format!("backlog:sorted:{backlog_key}"),
            "backlogs".into(),
            "shadow:sorted".into(),
            format!("shadow:sorted:{shadow_id}"),
            "shadows".into(),
            "accounts:shadows:sorted".into(),
            format!("accounts:{}:shadows:sorted", id.account_id),
            "backlog:sorted:-".into(),
            format!("normalize:{normalize}"),
            format!("normalize:accounts:{}:sorted", id.account_id),
            "normalize:sorted".into(),
            format!("singleton-run:{}", id.run_id),
            "singleton:-".into(),
            format!("idx:run:{}", id.run_id),
            format!("queue:status:{}:in-progress", id.function_id),
        ];
        let keys = suffixes
            .iter()
            .map(|s| format!("{{{}}}:{s}", d.queue.prefix))
            .collect();
        let shadow = if key_mode {
            encode(&Shadow {
                id: shadow_id,
                fv: d.function_version,
                fid: &id.function_id,
                eid: &id.env_id,
                aid: &id.account_id,
            })
        } else {
            br#"{"fv":0}"#.to_vec()
        };
        let backlog = if key_mode {
            encode(&Backlog {
                fv: d.function_version,
                id: &backlog_id,
                sid: shadow_id,
            })
        } else {
            b"{}".to_vec()
        };
        let args = vec![
            item,
            stored_id.as_bytes().to_vec(),
            score.to_string().into_bytes(),
            score
                .max(d.now_ms)
                .div_euclid(1000)
                .to_string()
                .into_bytes(),
            d.now_ms.to_string().into_bytes(),
            partition,
            id.function_id.as_bytes().to_vec(),
            id.account_id.as_bytes().to_vec(),
            id.run_id.as_bytes().to_vec(),
            if key_mode {
                b"1".to_vec()
            } else {
                b"0".to_vec()
            },
            shadow,
            backlog,
            backlog_id.into_bytes(),
            vec![],
        ];
        Ok(Self {
            keys,
            args,
            stored_id,
        })
    }
}

// Duplicate success must follow the persisted queue item's decoding contract,
// not just find a plausible runID in otherwise malformed JSON. These are JSON
// type checks, not enqueue admission: absent/null fields and unknown fields keep
// Go's zero-value/forward-compatible behavior.
pub(crate) fn existing_run_id(raw: &[u8]) -> Option<String> {
    let item: serde_json::Value = serde_json::from_slice(raw).ok()?;
    if !decoded(&item, "queue") {
        return None;
    }
    let run = field(field(field(&item, "data"), "identifier"), "runID");
    if run.is_null() {
        return Some("00000000000000000000000000".into());
    }
    go_ulid(run.as_str()?)
}

fn go_ulid(s: &str) -> Option<String> {
    // oklog/ulid's UnmarshalText uses its non-strict parser, not ParseStrict.
    let v = s.as_bytes();
    if v.len() != 26 || v[0] > b'7' {
        return None;
    }
    let alphabet = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let d: Vec<u8> = v
        .iter()
        .map(|c| {
            alphabet
                .iter()
                .position(|a| *a == c.to_ascii_uppercase())
                .map_or(255, |n| n as u8)
        })
        .collect();
    let mut bytes = [0_u8; 16];
    bytes[0] = (d[0] << 5) | d[1];
    for (offset, start) in [(1, 2), (6, 10), (11, 18)] {
        bytes[offset] = (d[start] << 3) | (d[start + 1] >> 2);
        bytes[offset + 1] = (d[start + 1] << 6) | (d[start + 2] << 1) | (d[start + 3] >> 4);
        bytes[offset + 2] = (d[start + 3] << 4) | (d[start + 4] >> 1);
        bytes[offset + 3] = (d[start + 4] << 7) | (d[start + 5] << 2) | (d[start + 6] >> 3);
        bytes[offset + 4] = (d[start + 6] << 5) | d[start + 7];
    }
    let mut n = u128::from_be_bytes(bytes);
    let mut out = [b'0'; 26];
    for c in out.iter_mut().rev() {
        *c = alphabet[(n & 31) as usize];
        n >>= 5;
    }
    String::from_utf8(out.to_vec()).ok()
}

fn field<'a>(v: &'a serde_json::Value, key: &str) -> &'a serde_json::Value {
    v.as_object()
        .and_then(|o| {
            o.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(key))
                .map(|(_, v)| v)
        })
        .unwrap_or(&serde_json::Value::Null)
}

fn go_uuid(s: &str) -> bool {
    // google/uuid accepts canonical, raw hex, braced and urn forms.
    let bytes = s.as_bytes();
    let bytes = if bytes.len() == 45 && bytes[..9].eq_ignore_ascii_case(b"urn:uuid:") {
        &bytes[9..]
    } else if bytes.len() == 38 {
        &bytes[1..37]
    } else {
        bytes
    };
    match bytes.len() {
        32 => bytes.iter().all(u8::is_ascii_hexdigit),
        36 => bytes.iter().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                *c == b'-'
            } else {
                c.is_ascii_hexdigit()
            }
        }),
        _ => false,
    }
}

fn decoded(v: &serde_json::Value, shape: &str) -> bool {
    use serde_json::Value;
    // Generated enum UnmarshalJSON runs even for null, unlike scalar fields.
    if shape == "parallel" {
        return v
            .as_str()
            .is_some_and(|s| ["none", "wait", "race"].contains(&s.to_ascii_lowercase().as_str()));
    }
    if shape == "singletonMode" {
        return v
            .as_str()
            .is_some_and(|s| ["skip", "cancel"].contains(&s.to_ascii_lowercase().as_str()));
    }
    if v.is_null() {
        return true;
    }
    if let Some(element) = shape.strip_prefix("[]") {
        return v
            .as_array()
            .is_some_and(|a| a.iter().all(|v| decoded(v, element)));
    }
    match shape {
        "string" => return v.is_string(),
        "int" => return v.as_i64().is_some(),
        "uint" => return v.as_u64().is_some(),
        "bool" => return v.is_boolean(),
        "ulid" => return v.as_str().and_then(go_ulid).is_some(),
        "uuid" => return v.as_str().is_some_and(go_uuid),
        "map" => return v.is_object(),
        _ => {}
    }
    let fields: &[(&str, &str)] = match shape {
        "queue" => &[
            ("id", "string"),
            ("pt", "int"),
            ("at", "int"),
            ("wt", "int"),
            ("wfID", "uuid"),
            ("wsID", "uuid"),
            ("leaseID", "ulid"),
            ("genID", "int"),
            ("data", "item"),
            ("queueID", "string"),
            ("ip", "int"),
            ("rf", "string"),
            ("rat", "int"),
            ("eat", "int"),
            ("sc", "int"),
            ("cl", "capacity"),
        ],
        "item" => &[
            ("groupID", "string"),
            ("wsID", "uuid"),
            ("kind", "string"),
            ("identifier", "identifier"),
            ("atts", "int"),
            ("maxAtts", "int"),
            ("metadata", "map"),
            ("qn", "string"),
            ("runinfo", "runinfo"),
            ("throttle", "throttle"),
            ("singleton", "singleton"),
            ("cck", "[]concurrency"),
            ("pf", "int"),
            ("pm", "parallel"),
            ("pck", "string"),
            ("sem", "[]semaphore"),
        ],
        "identifier" => &[
            ("runID", "ulid"),
            ("wID", "uuid"),
            ("wv", "int"),
            ("evtID", "ulid"),
            ("bID", "ulid"),
            ("eventIDs", "[]ulid"),
            ("key", "string"),
            ("aID", "uuid"),
            ("wsID", "uuid"),
            ("appID", "uuid"),
            ("oRunID", "ulid"),
            ("rID", "uuid"),
            ("pf", "int"),
            ("cck", "[]concurrency"),
            ("sem", "[]semaphore"),
        ],
        "edgePayload" => &[("edge", "edge")],
        "edge" => &[
            ("incoming", "string"),
            ("outgoing", "string"),
            ("gen", "string"),
            ("gen_name", "string"),
        ],
        "capacity" => &[("l", "ulid"), ("i", "int")],
        "concurrency" => &[("k", "string"), ("h", "string"), ("l", "int")],
        "semaphore" => &[
            ("id", "string"),
            ("uv", "string"),
            ("w", "int"),
            ("r", "int"),
        ],
        "throttle" => &[
            ("k", "string"),
            ("l", "int"),
            ("b", "int"),
            ("p", "int"),
            ("keh", "string"),
        ],
        "singleton" => &[("k", "string"), ("m", "singletonMode")],
        "runinfo" => &[
            ("Latency", "int"),
            ("SojournDelay", "int"),
            ("Priority", "uint"),
            ("QueueShardName", "string"),
            ("ContinueCount", "uint"),
            ("RefilledFromBacklog", "string"),
            ("CapacityLease", "capacity"),
            ("ScavengeCount", "int"),
        ],
        _ => return false,
    };
    let Value::Object(object) = v else {
        return false;
    };
    for (name, value) in object {
        if let Some((_, shape)) = fields
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(name))
            && !decoded(value, shape)
        {
            return false;
        }
    }
    if shape == "item" {
        let get = |key: &str| {
            object
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(key))
                .map(|(_, v)| v)
        };
        if let (Some(kind), Some(payload)) = (get("kind").and_then(Value::as_str), get("payload"))
            && ["start", "edge", "sleep", "edge-error"].contains(&kind)
            && !decoded(payload, "edgePayload")
        {
            return false;
        }
    }
    true
}
