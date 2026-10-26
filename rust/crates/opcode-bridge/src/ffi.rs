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

#[cfg(test)]
mod tests {
    use super::{contain_ffi_panic, opcode_eval_v1};
    use crate::{ErrorCode, Kind, decode_frame, encode_frame};
    use std::mem::MaybeUninit;

    #[repr(align(4))]
    struct AlignedBytes<const N: usize>([u8; N]);

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
