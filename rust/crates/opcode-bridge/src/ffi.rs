use std::any::Any;
use std::mem::{align_of, forget, size_of};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use super::{ErrorCode, MAX_FRAME_LEN, ProtocolError, evaluate_request_frame};

fn address_range(pointer: *const u8, length: usize) -> Result<(usize, usize), ProtocolError> {
    let start = pointer as usize;
    let end = start
        .checked_add(length)
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidFrame))?;
    Ok((start, end))
}

fn overlaps(left: (usize, usize), right: (usize, usize)) -> bool {
    left.0 < right.1 && right.0 < left.1
}

fn discard_panic_payload(payload: Box<dyn Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        // A panic payload destructor must not unwind across the C boundary.
        forget(secondary);
    }
}

fn contain_ffi_panic(operation: impl FnOnce() -> u32) -> u32 {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(status) => status,
        Err(payload) => {
            discard_panic_payload(payload);
            ErrorCode::Panic as u32
        }
    }
}

unsafe fn evaluate_pointers(
    request: *const u8,
    request_len: u32,
    output: *mut u8,
    output_capacity: u32,
    written: *mut u32,
) -> u32 {
    if written.is_null() || !(written as usize).is_multiple_of(align_of::<u32>()) {
        return ErrorCode::InvalidFrame as u32;
    }
    let written_start = written as usize;
    if written_start.checked_add(size_of::<u32>()).is_none() {
        return ErrorCode::InvalidFrame as u32;
    }

    // The caller owns this output scalar. Its pointer validity is part of the
    // C ABI contract; setting it first keeps all ordinary failures at zero.
    unsafe { ptr::write(written, 0) };

    let request_len = request_len as usize;
    let output_capacity = output_capacity as usize;
    if request.is_null()
        || output.is_null()
        || !(super::HEADER_LEN..=MAX_FRAME_LEN).contains(&request_len)
        || !(super::HEADER_LEN..=MAX_FRAME_LEN).contains(&output_capacity)
    {
        return ErrorCode::InvalidFrame as u32;
    }

    let request_range = match address_range(request, request_len) {
        Ok(range) => range,
        Err(error) => return error.code as u32,
    };
    let output_range = match address_range(output, output_capacity) {
        Ok(range) => range,
        Err(error) => return error.code as u32,
    };
    let written_range = (written_start, written_start + size_of::<u32>());
    if overlaps(request_range, output_range)
        || overlaps(request_range, written_range)
        || overlaps(output_range, written_range)
    {
        return ErrorCode::InvalidFrame as u32;
    }

    // SAFETY: caller validity is required by the ABI. Nulls, sizes, address
    // overflow, and all pairwise overlap are checked before reading the request.
    let request_bytes = unsafe { std::slice::from_raw_parts(request, request_len) };
    let result = match evaluate_request_frame(request_bytes) {
        Ok(result) => result,
        Err(error) => return error.code as u32,
    };
    if result.len() > output_capacity || result.len() > u32::MAX as usize {
        return ErrorCode::FrameTooLarge as u32;
    }

    // SAFETY: the result is initialized, its length fits the validated output
    // capacity, and pairwise range checks established that output is disjoint.
    // A raw copy only requires writable output bytes; their previous contents
    // do not need to be initialized.
    unsafe { ptr::copy_nonoverlapping(result.as_ptr(), output, result.len()) };
    unsafe { ptr::write(written, result.len() as u32) };
    0
}

/// Evaluate one binary PLAN frame synchronously and write one binary RESULT.
///
/// The request must be readable for its complete length. The output must be
/// writable for its complete capacity; its bytes need not be initialized on
/// entry. Request, output, and `written` ranges must not overlap for a successful
/// call. Rust retains no pointer after this function returns and invokes no
/// callbacks. A valid aligned `written` pointer is set to zero before other
/// arguments are validated and remains zero on errors.
///
/// # Safety
/// If `written` is non-null and aligned, it must be valid for writing one
/// `u32`. If request and output pointers pass validation and are disjoint from
/// `written` and each other, request must be valid for reads of `request_len`
/// bytes and output must be valid for writes of `output_capacity` bytes. Output
/// need not be initialized. Null or misaligned `written` pointers are rejected
/// before dereference.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opcode_eval_v1(
    request: *const u8,
    request_len: u32,
    output: *mut u8,
    output_capacity: u32,
    written: *mut u32,
) -> u32 {
    contain_ffi_panic(|| unsafe {
        evaluate_pointers(request, request_len, output, output_capacity, written)
    })
}

unsafe fn project_pointers(
    tags: *const u8,
    tag_count: u32,
    output: *mut u32,
    output_capacity: u32,
) -> u32 {
    let tag_count = tag_count as usize;
    let output_capacity = output_capacity as usize;
    if tag_count > super::MAX_OPCODES {
        return ErrorCode::TooManyOpcodes as u32;
    }
    if (tag_count != 0 && tags.is_null())
        || output.is_null()
        || !(output as usize).is_multiple_of(align_of::<u32>())
        || !(4..=4 + 2 * super::MAX_OPCODES).contains(&output_capacity)
    {
        return ErrorCode::InvalidPlan as u32;
    }
    let output_bytes = match output_capacity.checked_mul(size_of::<u32>()) {
        Some(bytes) => bytes,
        None => return ErrorCode::InvalidPlan as u32,
    };
    let output_range = match address_range(output.cast(), output_bytes) {
        Ok(range) => range,
        Err(error) => return error.code as u32,
    };
    // Empty input has no memory range and may use a null pointer.
    let tag_range = if tag_count == 0 {
        (0, 0)
    } else {
        match address_range(tags, tag_count) {
            Ok(range) => range,
            Err(error) => return error.code as u32,
        }
    };
    if overlaps(tag_range, output_range) {
        return ErrorCode::InvalidPlan as u32;
    }
    let raw_tags = if tag_count == 0 {
        &[]
    } else {
        // SAFETY: the caller guarantees readable initialized input. Bounds,
        // address overflow and overlap were checked before creating a slice.
        unsafe { std::slice::from_raw_parts(tags, tag_count) }
    };
    let mut decoded = Vec::new();
    if decoded.try_reserve_exact(tag_count).is_err() {
        return ErrorCode::AllocationFailed as u32;
    }
    for &raw in raw_tags {
        match super::OpcodeTag::try_from(raw) {
            Ok(tag) => decoded.push(tag),
            Err(_) => return ErrorCode::UnknownOpcode as u32,
        }
    }
    let projection = match super::project(&decoded) {
        Ok(projection) => projection,
        Err(error) => return ProtocolError::from(error).code as u32,
    };
    let groups = [
        projection.priority_indices.as_slice(),
        projection.other_indices.as_slice(),
        projection.non_lazy_indices.as_slice(),
    ];
    let required = groups
        .iter()
        .try_fold(4usize, |count, group| count.checked_add(group.len()));
    let Some(required) = required else {
        return ErrorCode::InvalidPlan as u32;
    };
    if required > output_capacity {
        return ErrorCode::FrameTooLarge as u32;
    }
    let flags = (u32::from(projection.history_group) * super::RESULT_FLAG_HISTORY_GROUP)
        | (u32::from(projection.has_plan_op) * super::RESULT_FLAG_HAS_PLAN_OP);
    let header = [
        groups[0].len() as u32,
        groups[1].len() as u32,
        groups[2].len() as u32,
        flags,
    ];
    // No output slice: the caller's storage may be uninitialized. Complete all
    // fallible work first, then copy only initialized values into the prefix.
    // SAFETY: writable aligned output and disjoint input are caller obligations
    // plus range checks above; required words fit the checked capacity.
    unsafe {
        ptr::copy_nonoverlapping(header.as_ptr(), output, header.len());
        let mut position = header.len();
        for group in groups {
            ptr::copy_nonoverlapping(group.as_ptr(), output.add(position), group.len());
            position += group.len();
        }
    }
    0
}

/// Project version-1 tag bytes into caller-owned native `uint32_t` words.
///
/// Capacity is in words, bounded by `4 + 2 * MAX_OPCODES`. On status zero,
/// output is `[priority_count, other_count, non_lazy_count, flags, indices...]`,
/// with priority, other, then non-lazy original indices. Exactly the first
/// `4 + priority_count + other_count + non_lazy_count` words are initialized;
/// the suffix is untouched. On any nonzero status no output is usable. Ordinary
/// validation/allocation errors leave output untouched. No pointer is retained,
/// no callbacks occur, and no Rust allocation ownership crosses this boundary.
/// This native-value ABI has no process framing, negotiation, or request ID.
///
/// # Safety
/// Nonempty input passing numeric checks must be readable and initialized for
/// `tag_count` bytes in one allocation. Output passing numeric checks must be
/// writable for `output_capacity` aligned `u32` words in one allocation; it need
/// not be initialized. Input and output must be disjoint for success. Null input
/// is permitted only for zero tags. Numeric validation cannot prove allocation
/// validity or prevent concurrent caller mutation. The caller must keep both
/// regions live and exclusively grant output access until this call returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opcode_project_v1(
    tags: *const u8,
    tag_count: u32,
    output: *mut u32,
    output_capacity: u32,
) -> u32 {
    contain_ffi_panic(|| unsafe { project_pointers(tags, tag_count, output, output_capacity) })
}

#[cfg(test)]
mod tests {
    use super::{contain_ffi_panic, opcode_eval_v1, opcode_project_v1};
    use crate::{ErrorCode, Kind, decode_frame, encode_frame};
    use std::mem::MaybeUninit;

    #[repr(align(4))]
    struct AlignedBytes<const N: usize>([u8; N]);

    #[test]
    fn direct_entry_initializes_only_the_result_prefix_in_native_words() {
        let tags = [5u8, 6, 11, 15, 2, 16];
        let mut output = [MaybeUninit::<u32>::uninit(); 20];
        // SAFETY: initialized input, aligned writable (uninitialized) output,
        // disjoint live allocations. Capacity is in words, not bytes.
        let status = unsafe { opcode_project_v1(tags.as_ptr(), 6, output.as_mut_ptr().cast(), 14) };
        assert_eq!(status, 0);
        // SAFETY: these fourteen words, and only these, are initialized by success.
        let result = unsafe { std::slice::from_raw_parts(output.as_ptr().cast::<u32>(), 14) };
        assert_eq!(result, [3, 3, 4, 3, 1, 3, 5, 0, 2, 4, 1, 0, 2, 4]);

        let mut output = [0xA5A5_A5A5u32; 20];
        // SAFETY: valid disjoint input/output, with extra capacity to verify suffix.
        assert_eq!(
            unsafe { opcode_project_v1(tags.as_ptr(), 6, output.as_mut_ptr(), 20) },
            0
        );
        assert_eq!(&output[14..], &[0xA5A5_A5A5; 6]);
        output.fill(0xA5A5_A5A5);
        // SAFETY: the valid output is deliberately one word too short.
        assert_eq!(
            unsafe { opcode_project_v1(tags.as_ptr(), 6, output.as_mut_ptr(), 13) },
            ErrorCode::FrameTooLarge as u32
        );
        assert_eq!(output, [0xA5A5_A5A5; 20]);

        // SAFETY: empty null input is permitted; four writable words suffice.
        assert_eq!(
            unsafe { opcode_project_v1(std::ptr::null(), 0, output.as_mut_ptr(), 4) },
            0
        );
        assert_eq!(&output[..4], &[0; 4]);
        assert_eq!(&output[4..], &[0xA5A5_A5A5; 16]);
    }

    #[test]
    fn direct_entry_rejects_invalid_regions_and_tags_without_writing() {
        use std::ptr::{null, null_mut};
        let tags = [2u8, 255];
        let mut output = [0xA5A5_A5A5u32; 16];
        let out = output.as_mut_ptr();
        let near_end = (usize::MAX - 3) as *mut u32;
        let cases = [
            (
                "null nonempty input",
                null(),
                1,
                out,
                16,
                ErrorCode::InvalidPlan,
            ),
            (
                "null output",
                tags.as_ptr(),
                1,
                null_mut(),
                16,
                ErrorCode::InvalidPlan,
            ),
            (
                "misaligned output",
                tags.as_ptr(),
                1,
                out.cast::<u8>().wrapping_add(1).cast(),
                8,
                ErrorCode::InvalidPlan,
            ),
            (
                "short header",
                tags.as_ptr(),
                1,
                out,
                3,
                ErrorCode::InvalidPlan,
            ),
            (
                "excess capacity",
                tags.as_ptr(),
                1,
                out,
                (4 + 2 * crate::MAX_OPCODES + 1) as u32,
                ErrorCode::InvalidPlan,
            ),
            (
                "excess count",
                tags.as_ptr(),
                (crate::MAX_OPCODES + 1) as u32,
                out,
                16,
                ErrorCode::TooManyOpcodes,
            ),
            (
                "input address overflow",
                (usize::MAX - 1) as *const u8,
                4,
                out,
                16,
                ErrorCode::InvalidFrame,
            ),
            (
                "output address overflow",
                tags.as_ptr(),
                1,
                near_end,
                4,
                ErrorCode::InvalidFrame,
            ),
            (
                "overlap at start",
                out.cast_const().cast(),
                1,
                out,
                16,
                ErrorCode::InvalidPlan,
            ),
            (
                "overlap at last byte",
                out.cast::<u8>().wrapping_add(63).cast_const(),
                1,
                out,
                16,
                ErrorCode::InvalidPlan,
            ),
            (
                "unknown tag",
                tags.as_ptr(),
                2,
                out,
                16,
                ErrorCode::UnknownOpcode,
            ),
        ];
        for (name, input, count, output_pointer, capacity, expected) in cases {
            // SAFETY: valid pointers back real allocations. Deliberately invalid
            // regions are rejected numerically before reads/writes or slices.
            let status = unsafe { opcode_project_v1(input, count, output_pointer, capacity) };
            assert_eq!(status, expected as u32, "{name}");
            assert_eq!(output, [0xA5A5_A5A5; 16], "{name}");
        }
    }

    #[test]
    fn direct_entry_handles_the_maximum_count_and_exact_capacity() {
        let count = crate::MAX_OPCODES;
        let tags = vec![2u8; count];
        let mut output = vec![u32::MAX; 4 + 2 * count + 1];
        // SAFETY: live initialized input and disjoint writable aligned output.
        assert_eq!(
            unsafe {
                opcode_project_v1(
                    tags.as_ptr(),
                    count as u32,
                    output.as_mut_ptr(),
                    (output.len() - 1) as u32,
                )
            },
            0
        );
        assert_eq!(&output[..4], &[0, count as u32, count as u32, 1]);
        for index in 0..count {
            assert_eq!(output[4 + index], index as u32);
            assert_eq!(output[4 + count + index], index as u32);
        }
        assert_eq!(output[4 + 2 * count], u32::MAX);
    }

    fn request() -> Vec<u8> {
        let payload = [1u32.to_le_bytes().as_slice(), &[6]].concat();
        encode_frame(Kind::Plan, 3, 0, &payload).unwrap()
    }

    #[test]
    fn c_entry_returns_the_same_result_frame_and_zeroes_written_on_errors() {
        let request = request();
        let mut output = vec![0u8; 128];
        let mut written = u32::MAX;
        // SAFETY: all three ranges are valid, correctly aligned, and disjoint.
        let status = unsafe {
            opcode_eval_v1(
                request.as_ptr(),
                request.len() as u32,
                output.as_mut_ptr(),
                output.len() as u32,
                &mut written,
            )
        };
        assert_eq!(status, 0);
        let result = decode_frame(&output[..written as usize]).unwrap();
        assert_eq!(result.kind, Kind::Result);
        assert_eq!(result.request_id, 3);

        let mut short = [0xA5; 24];
        written = u32::MAX;
        // SAFETY: request and output are valid and disjoint; the output is
        // deliberately too small for the result frame.
        let status = unsafe {
            opcode_eval_v1(
                request.as_ptr(),
                request.len() as u32,
                short.as_mut_ptr(),
                short.len() as u32,
                &mut written,
            )
        };
        assert_eq!(status, ErrorCode::FrameTooLarge as u32);
        assert_eq!(written, 0);
        assert_eq!(short, [0xA5; 24]);
    }

    #[test]
    fn c_entry_writes_into_uninitialized_output() {
        let request = request();
        let mut output = [MaybeUninit::<u8>::uninit(); 128];
        let mut written = u32::MAX;
        // SAFETY: request and output are valid, disjoint, and output is writable;
        // the ABI does not require its contents to be initialized on entry.
        let status = unsafe {
            opcode_eval_v1(
                request.as_ptr(),
                request.len() as u32,
                output.as_mut_ptr().cast::<u8>(),
                output.len() as u32,
                &mut written,
            )
        };
        assert_eq!(status, 0);

        // SAFETY: success initialized exactly the first `written` output bytes.
        let result_bytes =
            unsafe { std::slice::from_raw_parts(output.as_ptr().cast::<u8>(), written as usize) };
        let result = decode_frame(result_bytes).unwrap();
        assert_eq!(result.kind, Kind::Result);
        assert_eq!(result.request_id, 3);
    }

    #[test]
    fn c_entry_rejects_null_and_misaligned_written_pointers() {
        let request = request();
        let mut output = [0xA5; 128];
        // SAFETY: request and output are valid and disjoint; null `written` is
        // rejected before any pointer is dereferenced.
        let status = unsafe {
            opcode_eval_v1(
                request.as_ptr(),
                request.len() as u32,
                output.as_mut_ptr(),
                output.len() as u32,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(status, ErrorCode::InvalidFrame as u32);
        assert_eq!(output, [0xA5; 128]);

        let mut written_storage = AlignedBytes([0xFF; 8]);
        let misaligned_written = unsafe { written_storage.0.as_mut_ptr().add(1).cast::<u32>() };
        // SAFETY: request and output are valid and disjoint. The deliberately
        // misaligned `written` pointer is rejected before dereference.
        let status = unsafe {
            opcode_eval_v1(
                request.as_ptr(),
                request.len() as u32,
                output.as_mut_ptr(),
                output.len() as u32,
                misaligned_written,
            )
        };
        assert_eq!(status, ErrorCode::InvalidFrame as u32);
        assert_eq!(output, [0xA5; 128]);
        assert_eq!(written_storage.0, [0xFF; 8]);
    }

    #[test]
    fn c_entry_rejects_null_bounds_and_all_pairwise_overlaps() {
        let request = request();
        let mut output = [0u8; 128];
        let mut written = u32::MAX;
        // SAFETY: null request is intentionally rejected before dereference.
        let status = unsafe {
            opcode_eval_v1(
                std::ptr::null(),
                request.len() as u32,
                output.as_mut_ptr(),
                output.len() as u32,
                &mut written,
            )
        };
        assert_eq!(status, ErrorCode::InvalidFrame as u32);
        assert_eq!(written, 0);

        written = u32::MAX;
        // SAFETY: the valid request, valid written scalar, and null output are
        // handled before any output access.
        let status = unsafe {
            opcode_eval_v1(
                request.as_ptr(),
                request.len() as u32,
                std::ptr::null_mut(),
                output.len() as u32,
                &mut written,
            )
        };
        assert_eq!(status, ErrorCode::InvalidFrame as u32);
        assert_eq!(written, 0);

        let mut shared = [0u8; 256];
        shared[..request.len()].copy_from_slice(&request);
        written = u32::MAX;
        // SAFETY: both slices refer to valid memory, but their ranges overlap;
        // the ABI rejects them before constructing either slice.
        let status = unsafe {
            opcode_eval_v1(
                shared.as_ptr(),
                request.len() as u32,
                shared.as_mut_ptr().add(8),
                128,
                &mut written,
            )
        };
        assert_eq!(status, ErrorCode::InvalidFrame as u32);
        assert_eq!(written, 0);

        // SAFETY: the request storage is aligned and valid, output is valid and
        // disjoint, and the ABI zeros the valid written scalar before detecting
        // that its range aliases the request.
        let mut request_and_written = AlignedBytes([0; 256]);
        request_and_written.0[..request.len()].copy_from_slice(&request);
        let request_written = request_and_written.0.as_mut_ptr().cast::<u32>();
        let status = unsafe {
            opcode_eval_v1(
                request_and_written.0.as_ptr(),
                request.len() as u32,
                output.as_mut_ptr(),
                output.len() as u32,
                request_written,
            )
        };
        assert_eq!(status, ErrorCode::InvalidFrame as u32);
        assert_eq!(unsafe { request_written.read() }, 0);
        assert_eq!(output, [0u8; 128]);

        // SAFETY: request and the aligned output region are valid and disjoint;
        // `written` aliases output and is rejected before a result is copied.
        let mut output_and_written = AlignedBytes([0xA5; 256]);
        let output_written = output_and_written.0.as_mut_ptr().cast::<u32>();
        unsafe { output_written.write(u32::MAX) };
        let status = unsafe {
            opcode_eval_v1(
                request.as_ptr(),
                request.len() as u32,
                output_and_written.0.as_mut_ptr(),
                128,
                output_written,
            )
        };
        assert_eq!(status, ErrorCode::InvalidFrame as u32);
        assert_eq!(unsafe { output_written.read() }, 0);
        assert_eq!(&output_and_written.0[4..128], &[0xA5; 124]);

        written = u32::MAX;
        // SAFETY: the near-maximum request address and length overflow before
        // any slice is constructed; other pointers are valid and disjoint.
        let status = unsafe {
            opcode_eval_v1(
                (usize::MAX - 7) as *const u8,
                32,
                output.as_mut_ptr(),
                output.len() as u32,
                &mut written,
            )
        };
        assert_eq!(status, ErrorCode::InvalidFrame as u32);
        assert_eq!(written, 0);
    }

    #[test]
    fn entry_panic_is_contained_and_panic_payload_destruction_is_contained() {
        struct PanicsOnDrop;
        impl Drop for PanicsOnDrop {
            fn drop(&mut self) {
                panic!("payload destructor panic");
            }
        }

        let status = contain_ffi_panic(|| std::panic::panic_any(PanicsOnDrop));
        assert_eq!(status, ErrorCode::Panic as u32);
    }
}
