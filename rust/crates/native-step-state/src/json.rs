//! Go encoding/json RawMessage validation/compaction without numeric decoding.
//! Preserve duplicate keys, number lexemes, invalid UTF-8 inside strings and
//! escaped surrogate code units, as the Go scanner does. Never parse into Value.
use crate::PreparationError;

fn ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}
pub(crate) fn skip(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && ws(b[i]) {
        i += 1;
    }
    i
}
fn string(b: &[u8], mut i: usize) -> Result<usize, PreparationError> {
    if b.get(i) != Some(&b'"') {
        return Err(PreparationError::InvalidJson);
    }
    i += 1;
    while let Some(&v) = b.get(i) {
        match v {
            b'"' => return Ok(i + 1),
            0..=31 => return Err(PreparationError::InvalidJson),
            b'\\' => {
                i += 1;
                match b.get(i) {
                    Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => {}
                    Some(b'u') => {
                        for _ in 0..4 {
                            i += 1;
                            if !b.get(i).is_some_and(u8::is_ascii_hexdigit) {
                                return Err(PreparationError::InvalidJson);
                            }
                        }
                    }
                    _ => return Err(PreparationError::InvalidJson),
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(PreparationError::InvalidJson)
}
#[derive(Clone, Copy)]
enum Task {
    Value,
    ArrayFirst,
    ArrayNext,
    ArrayAfter,
    ObjectFirst,
    ObjectNext,
    ObjectAfter,
    Colon,
}

// Iterative so Go's 10,000 nesting limit does not exhaust the native call stack.
pub(crate) fn end(b: &[u8], start: usize) -> Result<usize, PreparationError> {
    use Task::*;
    let mut tasks = vec![Value];
    let mut i = start;
    let mut depth = 0;
    while let Some(task) = tasks.pop() {
        i = skip(b, i);
        let c = *b.get(i).ok_or(PreparationError::InvalidJson)?;
        match task {
            Value => match c {
                b'{' | b'[' => {
                    depth += 1;
                    if depth > 10_000 {
                        return Err(PreparationError::InvalidJson);
                    }
                    tasks.push(if c == b'{' { ObjectFirst } else { ArrayFirst });
                    i += 1;
                }
                b'"' => i = string(b, i)?,
                b't' | b'f' | b'n' => {
                    let literal: &[u8] = match c {
                        b't' => b"true",
                        b'f' => b"false",
                        _ => b"null",
                    };
                    if b.get(i..i + literal.len()) != Some(literal) {
                        return Err(PreparationError::InvalidJson);
                    }
                    i += literal.len();
                }
                b'-' | b'0'..=b'9' => {
                    if c == b'-' {
                        i += 1;
                    }
                    match b.get(i) {
                        Some(b'0') => i += 1,
                        Some(b'1'..=b'9') => {
                            while b.get(i).is_some_and(u8::is_ascii_digit) {
                                i += 1;
                            }
                        }
                        _ => return Err(PreparationError::InvalidJson),
                    }
                    if b.get(i) == Some(&b'.') {
                        i += 1;
                        let begin = i;
                        while b.get(i).is_some_and(u8::is_ascii_digit) {
                            i += 1;
                        }
                        if i == begin {
                            return Err(PreparationError::InvalidJson);
                        }
                    }
                    if matches!(b.get(i), Some(b'e' | b'E')) {
                        i += 1;
                        if matches!(b.get(i), Some(b'+' | b'-')) {
                            i += 1;
                        }
                        let begin = i;
                        while b.get(i).is_some_and(u8::is_ascii_digit) {
                            i += 1;
                        }
                        if i == begin {
                            return Err(PreparationError::InvalidJson);
                        }
                    }
                }
                _ => return Err(PreparationError::InvalidJson),
            },
            ArrayFirst if c == b']' => {
                i += 1;
                depth -= 1;
            }
            ArrayFirst | ArrayNext => {
                tasks.push(ArrayAfter);
                tasks.push(Value);
            }
            ArrayAfter => match c {
                b']' => {
                    i += 1;
                    depth -= 1;
                }
                b',' => {
                    i += 1;
                    tasks.push(ArrayNext);
                }
                _ => return Err(PreparationError::InvalidJson),
            },
            ObjectFirst if c == b'}' => {
                i += 1;
                depth -= 1;
            }
            ObjectFirst | ObjectNext => {
                i = string(b, i)?;
                tasks.push(ObjectAfter);
                tasks.push(Value);
                tasks.push(Colon);
            }
            Colon if c == b':' => i += 1,
            Colon => return Err(PreparationError::InvalidJson),
            ObjectAfter => match c {
                b'}' => {
                    i += 1;
                    depth -= 1;
                }
                b',' => {
                    i += 1;
                    tasks.push(ObjectNext);
                }
                _ => return Err(PreparationError::InvalidJson),
            },
        }
    }
    Ok(i)
}

// Retain only the compacted prefix needed by the prepared-output limit. Keep
// scanning emitted bytes after overflow so late AI markers retain precedence.
pub(crate) struct Compacted {
    pub(crate) bytes: Vec<u8>,
    pub(crate) oversized: bool,
    pub(crate) has_ai: bool,
    tail: [u8; 11],
    limit: usize,
}
impl Compacted {
    fn append(&mut self, bytes: &[u8]) {
        if !self.has_ai {
            for &byte in bytes {
                self.tail.copy_within(1.., 0);
                self.tail[10] = byte;
                if &self.tail[1..] == b"totalUsage" || &self.tail == b"inputTokens" {
                    self.has_ai = true;
                    break;
                }
            }
        }
        let retained = bytes.len().min(self.limit - self.bytes.len());
        self.oversized |= retained < bytes.len();
        let needed = self.bytes.len() + retained;
        if needed > self.bytes.capacity() {
            let capacity = needed
                .max(self.bytes.capacity().saturating_mul(2))
                .min(self.limit);
            self.bytes.reserve_exact(capacity - self.bytes.len());
        }
        self.bytes.extend_from_slice(&bytes[..retained]);
    }
    fn equals(&self, bytes: &[u8]) -> bool {
        !self.oversized && self.bytes == bytes
    }
}

pub(crate) fn compact_bounded(b: &[u8], limit: usize) -> Result<Compacted, PreparationError> {
    let start = skip(b, 0);
    let finish = end(b, start)?;
    if skip(b, finish) != b.len() {
        return Err(PreparationError::InvalidJson);
    }
    let mut out = Compacted {
        bytes: Vec::with_capacity(limit.min(finish - start)),
        oversized: false,
        has_ai: false,
        tail: [0; 11],
        limit,
    };
    let mut quoted = false;
    let mut i = start;
    while i < finish {
        let c = b[i];
        if quoted {
            match c {
                b'\\' => {
                    out.append(&b[i..i + 2]);
                    i += 2;
                    continue;
                }
                b'"' => quoted = false,
                b'<' => {
                    out.append(b"\\u003c");
                    i += 1;
                    continue;
                }
                b'>' => {
                    out.append(b"\\u003e");
                    i += 1;
                    continue;
                }
                b'&' => {
                    out.append(b"\\u0026");
                    i += 1;
                    continue;
                }
                0xe2 if b.get(i + 1) == Some(&0x80)
                    && matches!(b.get(i + 2), Some(0xa8 | 0xa9)) =>
                {
                    out.append(if b[i + 2] == 0xa8 {
                        b"\\u2028"
                    } else {
                        b"\\u2029"
                    });
                    i += 3;
                    continue;
                }
                _ => {}
            }
        } else if c == b'"' {
            quoted = true;
        } else if ws(c) {
            i += 1;
            continue;
        }
        out.append(&[c]);
        i += 1;
    }
    Ok(out)
}

// Enqueue serialization retains its existing unrestricted compaction contract.
pub(crate) fn compact(b: &[u8]) -> Result<Vec<u8>, PreparationError> {
    Ok(compact_bounded(b, usize::MAX)?.bytes)
}

// Strict accepted-response boundary. Unknown or nonempty unsupported fields
// are eligibility failures, never ignored SDK/AI/trace/experiment metadata.
pub(crate) fn response(raw: &[u8]) -> Result<(String, Option<&[u8]>), PreparationError> {
    let finish = end(raw, 0)?;
    if skip(raw, finish) != raw.len() {
        return Err(PreparationError::InvalidJson);
    }
    let mut i = skip(raw, 0);
    if raw.get(i) != Some(&b'[') {
        return Err(PreparationError::Ineligible);
    }
    i = skip(raw, i + 1);
    if raw.get(i) != Some(&b'{') {
        return Err(PreparationError::Ineligible);
    }
    i = skip(raw, i + 1);
    let mut seen = std::collections::HashSet::new();
    let mut id = None;
    let mut op = false;
    let mut data = None;
    while raw.get(i) != Some(&b'}') {
        let key_end = string(raw, i)?;
        let key: String =
            serde_json::from_slice(&raw[i..key_end]).map_err(|_| PreparationError::Ineligible)?;
        if !seen.insert(key.clone()) {
            return Err(PreparationError::Ineligible);
        }
        i = skip(raw, key_end) + 1; // colon already validated by end()
        i = skip(raw, i);
        let j = end(raw, i)?;
        let value = &raw[i..j];
        match key.as_str() {
            "op" => op = serde_json::from_slice::<String>(value).is_ok_and(|s| s == "StepRun"),
            "id" => {
                id = Some(
                    serde_json::from_slice::<String>(value)
                        .map_err(|_| PreparationError::Ineligible)?,
                )
            }
            "data" => data = Some(value),
            "opts" if compact_bounded(value, 2)?.equals(b"{}") || value == b"null" => {}
            "metadata" if compact_bounded(value, 2)?.equals(b"[]") || value == b"null" => {}
            "error" | "displayName" | "userland" if value == b"null" => {}
            // Ordinary SDK StepRun responses carry a name. It does not affect
            // output bytes or discovery identity; lifecycle remains Go-owned.
            "name" => {
                serde_json::from_slice::<String>(value)
                    .map_err(|_| PreparationError::Ineligible)?;
            }
            _ => return Err(PreparationError::Ineligible),
        }
        i = skip(raw, j);
        if raw.get(i) == Some(&b',') {
            i = skip(raw, i + 1);
        } else {
            break;
        }
    }
    i = skip(raw, i + 1);
    if raw.get(i) != Some(&b']') || skip(raw, i + 1) != raw.len() || !op {
        return Err(PreparationError::Ineligible);
    }
    let id = id
        .filter(|s| !s.is_empty())
        .ok_or(PreparationError::Ineligible)?;
    Ok((id, data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_retains_a_bounded_prefix_and_scans_past_it() {
        // Independent compacted bytes include expansion and removed whitespace.
        let raw = " [ \"<>&\u{2028}\u{2029}\", \"xxxxxxxxxxxxxxxx totalUsage\" ] ";
        let expected = br#"["\u003c\u003e\u0026\u2028\u2029","xxxxxxxxxxxxxxxx totalUsage"]"#;
        for limit in [0, 1, 17, expected.len() - 1, expected.len()] {
            let out = compact_bounded(raw.as_bytes(), limit).unwrap();
            assert_eq!(out.bytes, expected[..limit.min(expected.len())]);
            assert!(out.bytes.capacity() <= limit);
            assert_eq!(out.oversized, expected.len() > limit);
            assert!(
                out.has_ai,
                "AI marker after retained prefix must be detected"
            );
        }
        for marker in ["totalUsage", "inputTokens"] {
            for split in 0..=marker.len() {
                let raw = format!("\"{}{}\"", "x".repeat(32 - split), marker);
                let out = compact_bounded(raw.as_bytes(), 33).unwrap();
                assert!(out.has_ai, "{marker} split at {split}");
                assert!(out.oversized);
                assert_eq!(out.bytes.len(), 33);
                assert!(out.bytes.capacity() <= 33);
            }
        }
        assert!(!compact_bounded(br#""total\u0055sage""#, 1).unwrap().has_ai);
        assert!(matches!(
            compact_bounded(b"[\"xxxxxxxxxxxx\",]", 1),
            Err(PreparationError::InvalidJson)
        ));
    }
}
