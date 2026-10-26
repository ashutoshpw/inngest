//! Versioned binary transport and synchronous FFI for the opcode spike.
//!
//! Runtime dependencies are limited to the standard library and the pure
//! `opcode-kernel`. JSON is used only by tests to read the original Go fixture.

use std::fmt;
use std::io::{self, Read, Write};

use opcode_kernel::{KernelError, MAX_OPCODES, OpcodeTag, Projection, project};

pub const HEADER_LEN: usize = 24;
pub const MAX_FRAME_LEN: usize = 1_048_576;
pub const MAX_FRAME_PAYLOAD: usize = MAX_FRAME_LEN - HEADER_LEN;
pub const MAX_REQUEST_FRAME_LEN: usize = 65_564;
pub const MAX_RESULT_FRAME_LEN: usize = 524_328;
pub const MAX_RESULT_PAYLOAD_LEN: usize = MAX_RESULT_FRAME_LEN - HEADER_LEN;
pub const SCHEMA_VERSION: u16 = 1;
pub const INDEX_WIDTH: u16 = 4;
pub const RESULT_FLAG_HISTORY_GROUP: u32 = 1;
pub const RESULT_FLAG_HAS_PLAN_OP: u32 = 2;
const MAGIC: [u8; 4] = *b"IOPC";
const TAG_SET_VERSION: u16 = 1;
const SUPPORTED_FLAGS: u16 = 3;

/// Exact handshake schema and capabilities, echoed by READY.
pub const HANDSHAKE_PAYLOAD: [u8; 16] = [
    SCHEMA_VERSION as u8,
    (SCHEMA_VERSION >> 8) as u8,
    TAG_SET_VERSION as u8,
    (TAG_SET_VERSION >> 8) as u8,
    INDEX_WIDTH as u8,
    (INDEX_WIDTH >> 8) as u8,
    SUPPORTED_FLAGS as u8,
    (SUPPORTED_FLAGS >> 8) as u8,
    (MAX_OPCODES as u32) as u8,
    ((MAX_OPCODES as u32) >> 8) as u8,
    ((MAX_OPCODES as u32) >> 16) as u8,
    ((MAX_OPCODES as u32) >> 24) as u8,
    (MAX_FRAME_LEN as u32) as u8,
    ((MAX_FRAME_LEN as u32) >> 8) as u8,
    ((MAX_FRAME_LEN as u32) >> 16) as u8,
    ((MAX_FRAME_LEN as u32) >> 24) as u8,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum Kind {
    Hello = 1,
    Ready = 2,
    Plan = 3,
    Result = 4,
    Error = 5,
    Shutdown = 6,
    Bye = 7,
}

impl TryFrom<u16> for Kind {
    type Error = ProtocolError;

    fn try_from(value: u16) -> Result<Self, ProtocolError> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::Ready),
            3 => Ok(Self::Plan),
            4 => Ok(Self::Result),
            5 => Ok(Self::Error),
            6 => Ok(Self::Shutdown),
            7 => Ok(Self::Bye),
            _ => Err(ProtocolError::new(ErrorCode::UnknownKind)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum ErrorCode {
    InvalidFrame = 1,
    UnsupportedVersion = 2,
    UnknownKind = 3,
    InvalidHandshake = 4,
    InvalidPlan = 5,
    UnknownOpcode = 6,
    TooManyOpcodes = 7,
    FrameTooLarge = 8,
    AllocationFailed = 9,
    Panic = 10,
}

impl TryFrom<u32> for ErrorCode {
    type Error = ProtocolError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::InvalidFrame),
            2 => Ok(Self::UnsupportedVersion),
            3 => Ok(Self::UnknownKind),
            4 => Ok(Self::InvalidHandshake),
            5 => Ok(Self::InvalidPlan),
            6 => Ok(Self::UnknownOpcode),
            7 => Ok(Self::TooManyOpcodes),
            8 => Ok(Self::FrameTooLarge),
            9 => Ok(Self::AllocationFailed),
            10 => Ok(Self::Panic),
            _ => Err(ProtocolError::new(ErrorCode::InvalidFrame)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolError {
    pub code: ErrorCode,
}

impl ProtocolError {
    pub const fn new(code: ErrorCode) -> Self {
        Self { code }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "opcode bridge error {}", self.code as u32)
    }
}

impl std::error::Error for ProtocolError {}

impl From<KernelError> for ProtocolError {
    fn from(error: KernelError) -> Self {
        let code = match error {
            KernelError::TooManyOpcodes => ErrorCode::TooManyOpcodes,
            KernelError::AllocationFailed => ErrorCode::AllocationFailed,
        };
        Self::new(code)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Frame<'a> {
    pub kind: Kind,
    pub request_id: u64,
    pub status: u32,
    pub payload: &'a [u8],
}

pub fn encode_frame(
    kind: Kind,
    request_id: u64,
    status: u32,
    payload: &[u8],
) -> Result<Vec<u8>, ProtocolError> {
    let total_len = HEADER_LEN
        .checked_add(payload.len())
        .ok_or_else(|| ProtocolError::new(ErrorCode::FrameTooLarge))?;
    if total_len > MAX_FRAME_LEN || payload.len() > u32::MAX as usize {
        return Err(ProtocolError::new(ErrorCode::FrameTooLarge));
    }
    validate_header(kind, status, payload.len())?;

    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(total_len)
        .map_err(|_| ProtocolError::new(ErrorCode::AllocationFailed))?;
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(kind as u16).to_le_bytes());
    bytes.extend_from_slice(&request_id.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&status.to_le_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

pub fn decode_frame(bytes: &[u8]) -> Result<Frame<'_>, ProtocolError> {
    if bytes.len() < HEADER_LEN || bytes.len() > MAX_FRAME_LEN {
        return Err(ProtocolError::new(if bytes.len() > MAX_FRAME_LEN {
            ErrorCode::FrameTooLarge
        } else {
            ErrorCode::InvalidFrame
        }));
    }
    if bytes[0..4] != MAGIC {
        return Err(ProtocolError::new(ErrorCode::InvalidFrame));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != SCHEMA_VERSION {
        return Err(ProtocolError::new(ErrorCode::UnsupportedVersion));
    }
    let kind = Kind::try_from(u16::from_le_bytes([bytes[6], bytes[7]]))?;
    let request_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let payload_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let status = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    let total_len = HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| ProtocolError::new(ErrorCode::FrameTooLarge))?;
    if total_len > MAX_FRAME_LEN {
        return Err(ProtocolError::new(ErrorCode::FrameTooLarge));
    }
    if total_len != bytes.len() {
        return Err(ProtocolError::new(ErrorCode::InvalidFrame));
    }
    let payload = &bytes[HEADER_LEN..];
    validate_header(kind, status, payload.len())?;
    Ok(Frame {
        kind,
        request_id,
        status,
        payload,
    })
}

fn validate_header(kind: Kind, status: u32, payload_len: usize) -> Result<(), ProtocolError> {
    if kind == Kind::Error {
        if status == 0 || ErrorCode::try_from(status).is_err() || payload_len != 0 {
            return Err(ProtocolError::new(ErrorCode::InvalidFrame));
        }
    } else if status != 0 {
        return Err(ProtocolError::new(ErrorCode::InvalidFrame));
    }

    let valid_length = match kind {
        Kind::Hello | Kind::Ready => payload_len == HANDSHAKE_PAYLOAD.len(),
        Kind::Plan => (4..=4 + MAX_OPCODES).contains(&payload_len),
        Kind::Result => (16..=MAX_RESULT_PAYLOAD_LEN).contains(&payload_len),
        Kind::Error | Kind::Shutdown | Kind::Bye => payload_len == 0,
    };
    if !valid_length {
        return Err(ProtocolError::new(ErrorCode::InvalidFrame));
    }
    Ok(())
}

fn read_frame<R: Read>(reader: &mut R) -> Result<Option<Vec<u8>>, ProtocolError> {
    let mut header = [0u8; HEADER_LEN];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(1) => break,
            Ok(_) => unreachable!("single-byte read cannot return more than one byte"),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(ProtocolError::new(ErrorCode::InvalidFrame)),
        }
    }
    reader
        .read_exact(&mut header[1..])
        .map_err(|_| ProtocolError::new(ErrorCode::InvalidFrame))?;

    if header[0..4] != MAGIC {
        return Err(ProtocolError::new(ErrorCode::InvalidFrame));
    }
    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != SCHEMA_VERSION {
        return Err(ProtocolError::new(ErrorCode::UnsupportedVersion));
    }
    let kind = Kind::try_from(u16::from_le_bytes([header[6], header[7]]))?;
    let payload_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    let status = u32::from_le_bytes(header[20..24].try_into().unwrap());
    validate_header(kind, status, payload_len)?;
    let total_len = HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| ProtocolError::new(ErrorCode::FrameTooLarge))?;
    if total_len > MAX_FRAME_LEN {
        return Err(ProtocolError::new(ErrorCode::FrameTooLarge));
    }

    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(total_len)
        .map_err(|_| ProtocolError::new(ErrorCode::AllocationFailed))?;
    bytes.extend_from_slice(&header);
    bytes.resize(total_len, 0);
    reader
        .read_exact(&mut bytes[HEADER_LEN..])
        .map_err(|_| ProtocolError::new(ErrorCode::InvalidFrame))?;
    decode_frame(&bytes)?;
    Ok(Some(bytes))
}

fn write_frame<W: Write>(writer: &mut W, frame: &[u8]) -> Result<(), ProtocolError> {
    writer
        .write_all(frame)
        .and_then(|()| writer.flush())
        .map_err(|_| ProtocolError::new(ErrorCode::InvalidFrame))
}

fn decode_plan(payload: &[u8]) -> Result<Vec<OpcodeTag>, ProtocolError> {
    if payload.len() < 4 {
        return Err(ProtocolError::new(ErrorCode::InvalidPlan));
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    if count > MAX_OPCODES {
        return Err(ProtocolError::new(ErrorCode::TooManyOpcodes));
    }
    let expected_len = 4usize
        .checked_add(count)
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidPlan))?;
    if expected_len != payload.len() {
        return Err(ProtocolError::new(ErrorCode::InvalidPlan));
    }
    let mut tags = Vec::new();
    tags.try_reserve_exact(count)
        .map_err(|_| ProtocolError::new(ErrorCode::AllocationFailed))?;
    for raw_tag in payload[4..].iter().copied() {
        tags.push(
            OpcodeTag::try_from(raw_tag)
                .map_err(|_| ProtocolError::new(ErrorCode::UnknownOpcode))?,
        );
    }
    Ok(tags)
}

fn encode_result_payload(projection: &Projection) -> Result<Vec<u8>, ProtocolError> {
    let count_sum = projection
        .priority_indices
        .len()
        .checked_add(projection.other_indices.len())
        .and_then(|count| count.checked_add(projection.non_lazy_indices.len()))
        .ok_or_else(|| ProtocolError::new(ErrorCode::FrameTooLarge))?;
    if projection.priority_indices.len() > MAX_OPCODES
        || projection.other_indices.len() > MAX_OPCODES
        || projection.non_lazy_indices.len() > MAX_OPCODES
        || count_sum > MAX_OPCODES * 3
    {
        return Err(ProtocolError::new(ErrorCode::FrameTooLarge));
    }

    let index_count = projection
        .priority_indices
        .len()
        .checked_add(projection.other_indices.len())
        .and_then(|count| count.checked_add(projection.non_lazy_indices.len()))
        .ok_or_else(|| ProtocolError::new(ErrorCode::FrameTooLarge))?;
    let payload_len = 16usize
        .checked_add(
            index_count
                .checked_mul(INDEX_WIDTH as usize)
                .ok_or_else(|| ProtocolError::new(ErrorCode::FrameTooLarge))?,
        )
        .ok_or_else(|| ProtocolError::new(ErrorCode::FrameTooLarge))?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(payload_len)
        .map_err(|_| ProtocolError::new(ErrorCode::AllocationFailed))?;
    payload.extend_from_slice(&(projection.priority_indices.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(projection.other_indices.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(projection.non_lazy_indices.len() as u32).to_le_bytes());
    let flags = (u32::from(projection.history_group) * RESULT_FLAG_HISTORY_GROUP)
        | (u32::from(projection.has_plan_op) * RESULT_FLAG_HAS_PLAN_OP);
    decode_result_flags(flags)?;
    payload.extend_from_slice(&flags.to_le_bytes());
    for index in projection
        .priority_indices
        .iter()
        .chain(projection.other_indices.iter())
        .chain(projection.non_lazy_indices.iter())
    {
        payload.extend_from_slice(&index.to_le_bytes());
    }
    Ok(payload)
}

fn decode_result_flags(flags: u32) -> Result<(bool, bool), ProtocolError> {
    if flags & !(RESULT_FLAG_HISTORY_GROUP | RESULT_FLAG_HAS_PLAN_OP) != 0 {
        return Err(ProtocolError::new(ErrorCode::InvalidFrame));
    }
    Ok((
        flags & RESULT_FLAG_HISTORY_GROUP != 0,
        flags & RESULT_FLAG_HAS_PLAN_OP != 0,
    ))
}

fn encode_result_frame(request_id: u64, projection: &Projection) -> Result<Vec<u8>, ProtocolError> {
    let payload = encode_result_payload(projection)?;
    encode_frame(Kind::Result, request_id, 0, &payload)
}

pub fn evaluate_request_frame(request: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    if request.len() > MAX_REQUEST_FRAME_LEN {
        return Err(ProtocolError::new(ErrorCode::FrameTooLarge));
    }
    let frame = decode_frame(request)?;
    if frame.kind != Kind::Plan || frame.status != 0 || frame.request_id == 0 {
        return Err(ProtocolError::new(ErrorCode::InvalidPlan));
    }
    let tags = decode_plan(frame.payload)?;
    let projection = project(&tags).map_err(ProtocolError::from)?;
    encode_result_frame(frame.request_id, &projection)
}

fn write_error<W: Write>(
    writer: &mut W,
    request_id: u64,
    code: ErrorCode,
) -> Result<(), ProtocolError> {
    let bytes = encode_frame(Kind::Error, request_id, code as u32, &[])?;
    write_frame(writer, &bytes)
}

/// Run the single-threaded worker loop over standard input and output.
pub fn run_stdio() -> Result<(), ProtocolError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let mut handshaken = false;
    let mut last_request_id = 0u64;

    loop {
        let bytes = match read_frame(&mut input)? {
            Some(bytes) => bytes,
            None => return Ok(()),
        };
        let frame = decode_frame(&bytes)?;

        if !handshaken {
            if frame.kind != Kind::Hello
                || frame.request_id != 0
                || frame.status != 0
                || frame.payload != HANDSHAKE_PAYLOAD
            {
                write_error(&mut output, frame.request_id, ErrorCode::InvalidHandshake)?;
                return Err(ProtocolError::new(ErrorCode::InvalidHandshake));
            }
            let ready = encode_frame(Kind::Ready, 0, 0, &HANDSHAKE_PAYLOAD)?;
            write_frame(&mut output, &ready)?;
            handshaken = true;
            continue;
        }

        match frame.kind {
            Kind::Plan => {
                if frame.status != 0 || frame.request_id == 0 || frame.request_id <= last_request_id
                {
                    write_error(&mut output, frame.request_id, ErrorCode::InvalidPlan)?;
                    return Err(ProtocolError::new(ErrorCode::InvalidPlan));
                }
                last_request_id = frame.request_id;
                let result = match evaluate_request_frame(&bytes) {
                    Ok(result) => result,
                    Err(error) => {
                        write_error(&mut output, frame.request_id, error.code)?;
                        return Err(error);
                    }
                };
                write_frame(&mut output, &result)?;
            }
            Kind::Shutdown => {
                if frame.request_id != 0 || frame.status != 0 || !frame.payload.is_empty() {
                    write_error(&mut output, frame.request_id, ErrorCode::InvalidFrame)?;
                    return Err(ProtocolError::new(ErrorCode::InvalidFrame));
                }
                let bye = encode_frame(Kind::Bye, 0, 0, &[])?;
                write_frame(&mut output, &bye)?;
                return Ok(());
            }
            _ => {
                write_error(&mut output, frame.request_id, ErrorCode::UnknownKind)?;
                return Err(ProtocolError::new(ErrorCode::UnknownKind));
            }
        }
    }
}

mod ffi;
pub use ffi::opcode_eval_v1;

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::{
        ErrorCode, HANDSHAKE_PAYLOAD, Kind, MAX_REQUEST_FRAME_LEN, ProtocolError, decode_frame,
        decode_result_flags, encode_frame, evaluate_request_frame,
    };
    use opcode_kernel::{MAX_OPCODES, OpcodeTag, Projection, project};

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Fixture {
        schema_version: u32,
        contract: String,
        source: FixtureSource,
        cases: Vec<FixtureCase>,
    }

    #[derive(Deserialize)]
    struct FixtureSource {
        commit: String,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FixtureCase {
        name: String,
        input: Vec<FixtureOpcode>,
        expected: FixtureExpected,
    }

    #[derive(Deserialize)]
    struct FixtureOpcode {
        op: String,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FixtureExpected {
        priority_indices: Vec<u32>,
        other_indices: Vec<u32>,
        non_lazy_indices: Vec<u32>,
        history_group: bool,
        has_plan_op: bool,
    }

    fn fixture_tag(name: &str) -> Option<OpcodeTag> {
        match name {
            "None" => Some(OpcodeTag::None),
            "Step" => Some(OpcodeTag::Step),
            "StepRun" => Some(OpcodeTag::StepRun),
            "StepError" => Some(OpcodeTag::StepError),
            "StepPlanned" => Some(OpcodeTag::StepPlanned),
            "Sleep" => Some(OpcodeTag::Sleep),
            "WaitForEvent" => Some(OpcodeTag::WaitForEvent),
            "InvokeFunction" => Some(OpcodeTag::InvokeFunction),
            "AIGateway" => Some(OpcodeTag::AIGateway),
            "Gateway" => Some(OpcodeTag::Gateway),
            "WaitForSignal" => Some(OpcodeTag::WaitForSignal),
            "RunComplete" => Some(OpcodeTag::RunComplete),
            "StepFailed" => Some(OpcodeTag::StepFailed),
            "SyncRunComplete" => Some(OpcodeTag::SyncRunComplete),
            "DiscoveryRequest" => Some(OpcodeTag::DiscoveryRequest),
            "DeferAdd" => Some(OpcodeTag::DeferAdd),
            "DeferAbort" => Some(OpcodeTag::DeferAbort),
            _ => None,
        }
    }

    #[test]
    fn original_27_case_fixture_is_the_rust_oracle() {
        let fixture: Fixture = serde_json::from_str(include_str!(
            "../../../../pkg/execution/executor/testdata/compatibility/opcode-plan-v1.json"
        ))
        .expect("original checked-in Go fixture parses");
        assert_eq!(fixture.schema_version, 1);
        assert_eq!(fixture.contract, "opcode-plan-v1");
        assert_eq!(
            fixture.source.commit,
            "3624c37d5285805d2a95efd0715e8913d5f81ae8"
        );
        assert_eq!(fixture.cases.len(), 27);

        let mut tags_seen = [false; 17];
        for case in fixture.cases {
            let tags = case
                .input
                .iter()
                .map(|opcode| fixture_tag(&opcode.op).expect("recognized fixture opcode"))
                .collect::<Vec<_>>();
            for tag in &tags {
                tags_seen[*tag as usize] = true;
            }
            let actual = project(&tags).expect("fixture request is within the protocol limit");
            assert_eq!(
                actual.priority_indices, case.expected.priority_indices,
                "{}",
                case.name
            );
            assert_eq!(
                actual.other_indices, case.expected.other_indices,
                "{}",
                case.name
            );
            assert_eq!(
                actual.non_lazy_indices, case.expected.non_lazy_indices,
                "{}",
                case.name
            );
            assert_eq!(
                actual.history_group, case.expected.history_group,
                "{}",
                case.name
            );
            assert_eq!(
                actual.has_plan_op, case.expected.has_plan_op,
                "{}",
                case.name
            );
        }
        assert!(tags_seen.into_iter().all(|seen| seen));
    }

    #[test]
    fn request_and_result_share_versioned_frames() {
        let request_payload = [2u32.to_le_bytes().as_slice(), &[6, 2]].concat();
        let request = encode_frame(Kind::Plan, 17, 0, &request_payload).unwrap();
        assert_eq!(request.len(), 30);
        let result = evaluate_request_frame(&request).unwrap();
        let frame = decode_frame(&result).unwrap();
        assert_eq!(frame.kind, Kind::Result);
        assert_eq!(frame.request_id, 17);
        assert_eq!(frame.status, 0);
        assert_eq!(frame.payload.len(), 16 + (4 * 4));
    }

    #[test]
    fn frame_decoder_rejects_trailing_unknown_and_oversized_frames() {
        let mut valid = encode_frame(Kind::Plan, 1, 0, &[0, 0, 0, 0]).unwrap();
        valid.push(0);
        assert_eq!(
            decode_frame(&valid).unwrap_err().code,
            ErrorCode::InvalidFrame
        );

        let mut unknown = encode_frame(Kind::Plan, 1, 0, &[0, 0, 0, 0]).unwrap();
        unknown[6..8].copy_from_slice(&99u16.to_le_bytes());
        assert_eq!(
            decode_frame(&unknown).unwrap_err().code,
            ErrorCode::UnknownKind
        );

        let too_large = vec![0; super::MAX_FRAME_LEN + 1];
        assert_eq!(
            decode_frame(&too_large).unwrap_err().code,
            ErrorCode::FrameTooLarge
        );

        let truncated = &valid[..valid.len() - 2];
        assert_eq!(
            decode_frame(truncated).unwrap_err().code,
            ErrorCode::InvalidFrame
        );
    }

    #[test]
    fn frame_decoder_rejects_unknown_version_and_result_flags_are_reserved() {
        let mut unknown_version = encode_frame(Kind::Plan, 1, 0, &[0, 0, 0, 0]).unwrap();
        unknown_version[4..6].copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(
            decode_frame(&unknown_version).unwrap_err().code,
            ErrorCode::UnsupportedVersion
        );

        assert_eq!(decode_result_flags(0).unwrap(), (false, false));
        assert_eq!(decode_result_flags(3).unwrap(), (true, true));
        assert_eq!(
            decode_result_flags(4).unwrap_err().code,
            ErrorCode::InvalidFrame
        );
        assert_eq!(HANDSHAKE_PAYLOAD.len(), 16);
    }

    #[test]
    fn request_limits_and_unknown_tags_are_fixed_errors() {
        let too_many = [
            ((MAX_OPCODES + 1) as u32).to_le_bytes().as_slice(),
            &vec![0; MAX_OPCODES + 1],
        ]
        .concat();
        assert_eq!(
            super::decode_plan(&too_many).unwrap_err().code,
            ErrorCode::TooManyOpcodes
        );

        let maximum = [
            (MAX_OPCODES as u32).to_le_bytes().as_slice(),
            &vec![0; MAX_OPCODES],
        ]
        .concat();
        let request = encode_frame(Kind::Plan, 1, 0, &maximum).unwrap();
        assert_eq!(request.len(), MAX_REQUEST_FRAME_LEN);

        let unknown_tag = [1u32.to_le_bytes().as_slice(), &[255]].concat();
        let request = encode_frame(Kind::Plan, 1, 0, &unknown_tag).unwrap();
        assert_eq!(
            evaluate_request_frame(&request).unwrap_err().code,
            ErrorCode::UnknownOpcode
        );
    }

    #[test]
    fn result_frame_maximum_matches_the_contract() {
        let tags = vec![OpcodeTag::None; MAX_OPCODES];
        let projection: Projection = project(&tags).unwrap();
        let request = encode_frame(
            Kind::Plan,
            1,
            0,
            &[
                u32::to_le_bytes(MAX_OPCODES as u32).as_slice(),
                &vec![0; MAX_OPCODES],
            ]
            .concat(),
        )
        .unwrap();
        assert_eq!(request.len(), super::MAX_REQUEST_FRAME_LEN);
        let result = evaluate_request_frame(&request).unwrap();
        assert_eq!(result.len(), super::MAX_RESULT_FRAME_LEN);
        assert_eq!(projection.other_indices.len(), MAX_OPCODES);
    }

    #[test]
    fn errors_have_numeric_status_and_no_message_payload() {
        let error = encode_frame(Kind::Error, 9, ErrorCode::InvalidPlan as u32, &[]).unwrap();
        let decoded = decode_frame(&error).unwrap();
        assert_eq!(decoded.status, ErrorCode::InvalidPlan as u32);
        assert!(decoded.payload.is_empty());
        assert_eq!(
            encode_frame(Kind::Error, 9, ErrorCode::InvalidPlan as u32, b"details").unwrap_err(),
            ProtocolError::new(ErrorCode::InvalidFrame)
        );
    }
}
