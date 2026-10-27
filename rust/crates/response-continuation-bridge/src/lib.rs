//! Experimental, synchronous, I/O-free owned-session ABI. No Go callbacks.
use std::any::Any;
use std::collections::BTreeMap;
use std::mem::{align_of, forget, size_of};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::{Arc, Mutex, OnceLock};

use response_continuation::{Disposition, Input, Key, Observation, Progress, Request, Session};
use serde::{Deserialize, Serialize};

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_SESSIONS: usize = 64;
const MAX_OUTPUTS: usize = 64;
const INVALID: u32 = 1;
const UNKNOWN: u32 = 2;
const REJECTED: u32 = 3;
const BOUND: u32 = 4;
const PANIC: u32 = 5;
const POISONED: u32 = 6;

struct Entry {
    generation: u64,
    state: Mutex<Option<Session>>,
}
#[derive(Default)]
struct Registry {
    next: u64,
    sessions: BTreeMap<u64, Arc<Entry>>,
    outputs: BTreeMap<u64, Box<[u8]>>,
}
impl Registry {
    fn id(&mut self) -> Result<u64, u32> {
        self.next = self.next.checked_add(1).ok_or(BOUND)?;
        Ok(self.next)
    }
}
fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}
fn discard(payload: Box<dyn Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        forget(secondary);
    }
}
fn contain(f: impl FnOnce() -> Result<(), u32>) -> u32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => 0,
        Ok(Err(status)) => status,
        Err(payload) => {
            discard(payload);
            PANIC
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resume {
    key: Key,
    observation: Observation,
}
#[derive(Serialize)]
struct Output {
    ready: Vec<Request>,
    done: bool,
    completion: Option<Disposition>,
}
fn encode(p: Progress) -> Result<Box<[u8]>, u32> {
    let out = Output {
        ready: p.ready,
        done: p.completion.is_some(),
        completion: p.completion.flatten(),
    };
    let bytes = serde_json::to_vec(&out).map_err(|_| INVALID)?;
    if bytes.len() > MAX_BYTES {
        return Err(BOUND);
    }
    Ok(bytes.into_boxed_slice())
}

/// All scalar fields are initialized on every ordinary return. `data` is read
/// only and live until output_release(output); copying it does not transfer it.
#[repr(C)]
pub struct NativeOutput {
    pub session: u64,
    pub output: u64,
    pub data: *const u8,
    pub len: u32,
}
impl Default for NativeOutput {
    fn default() -> Self {
        Self {
            session: 0,
            output: 0,
            data: ptr::null(),
            len: 0,
        }
    }
}
fn range(p: usize, n: usize) -> Result<(usize, usize), u32> {
    Ok((p, p.checked_add(n).ok_or(INVALID)?))
}
// This reads input bytes only, never forms a slice over the writable output.
unsafe fn arguments<'a>(
    data: *const u8,
    len: u32,
    out: *mut NativeOutput,
) -> Result<&'a [u8], u32> {
    if out.is_null() || !(out as usize).is_multiple_of(align_of::<NativeOutput>()) {
        return Err(INVALID);
    }
    let output = range(out as usize, size_of::<NativeOutput>())?;
    if data.is_null() || len == 0 || len as usize > MAX_BYTES {
        unsafe { ptr::write(out, NativeOutput::default()) };
        return Err(INVALID);
    }
    let input = range(data as usize, len as usize)?;
    if input.0 < output.1 && output.0 < input.1 {
        return Err(INVALID);
    }
    unsafe { ptr::write(out, NativeOutput::default()) };
    // SAFETY: caller guarantees readable input; size, overflow and disjoint
    // aligned writable output have been checked. No pointer is retained.
    Ok(unsafe { std::slice::from_raw_parts(data, len as usize) })
}
fn publish(reg: &mut Registry, session: u64, bytes: Box<[u8]>) -> Result<NativeOutput, u32> {
    if reg.outputs.len() >= MAX_OUTPUTS {
        return Err(BOUND);
    }
    let output = reg.id()?;
    let result = NativeOutput {
        session,
        output,
        data: bytes.as_ptr(),
        len: bytes.len() as u32,
    };
    reg.outputs.insert(output, bytes);
    Ok(result)
}

/// Start one bounded session. JSON owns all input, including descriptor bytes.
///
/// # Safety
/// `data` must be readable for len bytes; out must be aligned and writable for
/// one NativeOutput. Ranges must be live, disjoint, and exclusively writable
/// where applicable for this call. No Go pointers/callbacks are retained.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn response_continuation_start_v1(
    data: *const u8,
    len: u32,
    out: *mut NativeOutput,
) -> u32 {
    contain(|| {
        let bytes = unsafe { arguments(data, len, out)? };
        let input: Input = serde_json::from_slice(bytes).map_err(|_| INVALID)?;
        let generation = input.generation;
        let (state, progress) = Session::start(input).map_err(|_| REJECTED)?;
        let bytes = encode(progress)?;
        let mut reg = registry().lock().map_err(|_| POISONED)?;
        if reg.sessions.len() >= MAX_SESSIONS
            || reg.sessions.values().any(|e| e.generation == generation)
        {
            return Err(BOUND);
        }
        let session = reg.id()?;
        let output = publish(&mut reg, session, bytes)?;
        reg.sessions.insert(
            session,
            Arc::new(Entry {
                generation,
                state: Mutex::new(Some(state)),
            }),
        );
        unsafe { ptr::write(out, output) };
        Ok(())
    })
}

/// Resume exactly one outstanding key. A nonzero status never permits replay
/// of an external effect. Rejected keys do not consume model state; any native
/// panic or failure after consumption poisons the session.
///
/// # Safety
/// Same pointer contract as start. Calls using a session must be serialized
/// with close. No I/O occurs while native/session/registry locks are held.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn response_continuation_resume_v1(
    session: u64,
    data: *const u8,
    len: u32,
    out: *mut NativeOutput,
) -> u32 {
    contain(|| {
        let bytes = unsafe { arguments(data, len, out)? };
        let resume: Resume = serde_json::from_slice(bytes).map_err(|_| INVALID)?;
        let entry = registry()
            .lock()
            .map_err(|_| POISONED)?
            .sessions
            .get(&session)
            .cloned()
            .ok_or(UNKNOWN)?;
        let mut state = entry.state.lock().map_err(|_| POISONED)?;
        let result = catch_unwind(AssertUnwindSafe(|| {
            let state = state.as_mut().ok_or(POISONED)?;
            state
                .resume(resume.key, resume.observation)
                .map_err(|_| REJECTED)
        }));
        let progress = match result {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return Err(e),
            Err(payload) => {
                *state = None;
                discard(payload);
                return Err(PANIC);
            }
        };
        let output = encode(progress).and_then(|bytes| {
            let mut reg = registry().lock().map_err(|_| POISONED)?;
            publish(&mut reg, session, bytes)
        });
        match output {
            Ok(output) => {
                unsafe { ptr::write(out, output) };
                Ok(())
            }
            Err(e) => {
                *state = None;
                Err(e)
            }
        }
    })
}

/// Release one Rust allocation. Handles are never reused; double release fails.
#[unsafe(no_mangle)]
pub extern "C" fn response_continuation_output_release_v1(output: u64) -> u32 {
    contain(|| {
        let allocation = registry()
            .lock()
            .map_err(|_| POISONED)?
            .outputs
            .remove(&output)
            .ok_or(UNKNOWN)?;
        drop(allocation);
        Ok(())
    })
}
/// Close only releases native state. The host must first join issued Go work
/// and separately drain async listeners; this ABI never owns those effects.
#[unsafe(no_mangle)]
pub extern "C" fn response_continuation_close_v1(session: u64) -> u32 {
    contain(|| {
        let entry = registry()
            .lock()
            .map_err(|_| POISONED)?
            .sessions
            .remove(&session)
            .ok_or(UNKNOWN)?;
        drop(entry);
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use response_continuation::{Config, Edge, Opcode, ParallelMode, QueueBindings};
    use std::mem::MaybeUninit;

    fn input(generation: u64) -> Input {
        Input {
            generation,
            ops: vec![Some(Opcode {
                tag: 2,
                id: "original-step".into(),
                name: "original-name".into(),
                display_name: None,
                data: b"null".to_vec(),
                options: Vec::new(),
                error: None,
                parallel_mode: ParallelMode::None,
                has_ai: false,
            })],
            config: Config {
                request_version: 2,
                ..Config::default()
            },
            response_version: 2,
            response_no_retry: false,
            response_user_no_retry: false,
            retry_at: None,
            run_id: "run".into(),
            idempotency_key: "key".into(),
            edge: Some(Edge::default()),
            attempt: 0,
            max_attempts: 3,
            context_mode: ParallelMode::None,
            context_group: "incoming".into(),
            context_coalesce_key: None,
            queue: QueueBindings::default(),
            handled_step_error: 1,
            function_overflow: 2,
        }
    }
    fn start(generation: u64) -> NativeOutput {
        let mut bytes = serde_json::to_vec(&input(generation)).unwrap();
        // The ABI must initialize writable uninitialized scalar output storage.
        let mut out = MaybeUninit::<NativeOutput>::uninit();
        assert_eq!(
            unsafe {
                response_continuation_start_v1(bytes.as_ptr(), bytes.len() as u32, out.as_mut_ptr())
            },
            0
        );
        bytes.fill(b' '); // no borrowed input survives Start.
        unsafe { out.assume_init() }
    }
    fn copy(out: &NativeOutput) -> serde_json::Value {
        assert!(!out.data.is_null());
        let bytes = unsafe { std::slice::from_raw_parts(out.data, out.len as usize) }.to_vec();
        serde_json::from_slice(&bytes).unwrap()
    }
    fn resume(
        session: u64,
        key: &serde_json::Value,
        observation: serde_json::Value,
    ) -> (u32, NativeOutput) {
        let bytes =
            serde_json::to_vec(&serde_json::json!({"key":key,"observation":observation})).unwrap();
        let mut out = NativeOutput::default();
        let status = unsafe {
            response_continuation_resume_v1(session, bytes.as_ptr(), bytes.len() as u32, &mut out)
        };
        (status, out)
    }

    // One primary native-ABI owner: copied inputs/output lifetime, session/key
    // isolation, consumed-once progress, bounds and post-consumption poisoning.
    #[test]
    fn owned_abi_sessions_correlation_bounds_and_poison() {
        let first = start(101);
        let session = first.session;
        let original = copy(&first);
        assert_eq!(response_continuation_output_release_v1(first.output), 0);
        assert_eq!(
            response_continuation_output_release_v1(first.output),
            UNKNOWN
        );
        let key = &original["ready"][0]["key"];
        let same_generation = serde_json::to_vec(&input(101)).unwrap();
        let mut collision = NativeOutput::default();
        assert_eq!(
            unsafe {
                response_continuation_start_v1(
                    same_generation.as_ptr(),
                    same_generation.len() as u32,
                    &mut collision,
                )
            },
            BOUND
        );
        assert_eq!(collision.output, 0);
        let mut stale = key.clone();
        stale["generation"] = 102.into();
        assert_eq!(
            resume(session, &stale, serde_json::json!({"Clock":0})).0,
            REJECTED
        );
        let other = start(102);
        assert_eq!(
            resume(other.session, key, serde_json::json!({"Clock":0})).0,
            REJECTED
        );
        assert_eq!(response_continuation_close_v1(other.session), 0);
        // Closing a session does not invalidate an already returned allocation.
        assert_eq!(copy(&other)["ready"][0]["key"]["generation"], 102);
        assert_eq!(response_continuation_output_release_v1(other.output), 0);
        let (status, active) = resume(session, key, serde_json::json!({"Clock":0}));
        assert_eq!(status, 0);
        assert_eq!(
            resume(session, key, serde_json::json!({"Clock":0})).0,
            REJECTED
        );
        let active_json = copy(&active);
        assert!(
            active_json["ready"][0]["effect"]
                .get("ActivateBranch")
                .is_some()
        );
        assert_eq!(response_continuation_output_release_v1(active.output), 0);
        let (status, prepared) = resume(
            session,
            &active_json["ready"][0]["key"],
            serde_json::json!("Done"),
        );
        assert_eq!(status, 0);
        let prepared_json = copy(&prepared);
        assert_eq!(
            prepared_json["ready"][0]["effect"]["PrepareSave"]["opcode"]["name"],
            "original-name"
        );
        assert_eq!(response_continuation_close_v1(session), 0);
        assert_eq!(response_continuation_close_v1(session), UNKNOWN);
        assert_eq!(response_continuation_output_release_v1(prepared.output), 0);
        assert_eq!(
            resume(session, key, serde_json::json!({"Clock":0})).0,
            UNKNOWN
        );

        let mut invalid = input(200);
        invalid.ops.push(Some(Opcode {
            tag: 5,
            ..invalid.ops[0].clone().unwrap()
        }));
        let bytes = serde_json::to_vec(&invalid).unwrap();
        let mut out = NativeOutput::default();
        assert_eq!(
            unsafe { response_continuation_start_v1(bytes.as_ptr(), bytes.len() as u32, &mut out) },
            REJECTED
        );
        assert_eq!(out.output, 0);
        assert_eq!(
            unsafe { response_continuation_start_v1(ptr::null(), u32::MAX, &mut out) },
            INVALID
        );
        assert_eq!(out.session, 0);

        // This is readable, valid JSON with trailing whitespace, so removing
        // the byte bound would actually admit it (not fail a different guard).
        let mut oversized = serde_json::to_vec(&input(201)).unwrap();
        oversized.resize(MAX_BYTES + 1, b' ');
        assert_eq!(
            unsafe {
                response_continuation_start_v1(oversized.as_ptr(), oversized.len() as u32, &mut out)
            },
            INVALID
        );
        assert_eq!(out.output, 0);

        let held = (0..MAX_OUTPUTS)
            .map(|i| start(300 + i as u64))
            .collect::<Vec<_>>();
        let held_json = copy(&held[0]);
        // Resume consumes Handling, then cannot publish its ready set because
        // all output slots are held. This poisons; it cannot replay Handling.
        assert_eq!(
            resume(
                held[0].session,
                &held_json["ready"][0]["key"],
                serde_json::json!({"Clock":0})
            )
            .0,
            BOUND
        );
        assert_eq!(response_continuation_output_release_v1(held[0].output), 0);
        assert_eq!(
            resume(
                held[0].session,
                &held_json["ready"][0]["key"],
                serde_json::json!({"Clock":0})
            )
            .0,
            POISONED
        );
        let handles = held.iter().map(|out| out.session).collect::<Vec<_>>();
        for (i, out) in held.into_iter().enumerate() {
            if i != 0 {
                assert_eq!(response_continuation_output_release_v1(out.output), 0);
            }
        }
        // All output slots are free but all sessions remain owned: exercise
        // the session limit independently from the output-allocation limit.
        let extra = serde_json::to_vec(&input(999)).unwrap();
        assert_eq!(
            unsafe { response_continuation_start_v1(extra.as_ptr(), extra.len() as u32, &mut out) },
            BOUND
        );
        assert_eq!(out.session, 0);
        for handle in handles {
            assert_eq!(response_continuation_close_v1(handle), 0);
        }
        // Repeated allocation/close beyond the registry bounds demonstrates
        // release, rather than a test-only registry/introspection export.
        for i in 0..MAX_SESSIONS + 2 {
            let out = start(1000 + i as u64);
            assert_eq!(response_continuation_output_release_v1(out.output), 0);
            assert_eq!(response_continuation_close_v1(out.session), 0);
        }
        assert_eq!(contain(|| panic!("contained native panic")), PANIC);
    }
}
