//go:build linux && opcode_bridge_spike && opcode_bridge_ffi && cgo

package opcodespike

/*
#include <stdint.h>

// `output` must be writable but need not be initialized on entry. For a
// successful call, request, output, and `written` ranges must not overlap.
uint32_t opcode_eval_v1(
    const uint8_t *request,
    uint32_t request_len,
    uint8_t *output,
    uint32_t output_capacity,
    uint32_t *written);

// Native-value ABI: capacity is in uint32_t words, bounded by 4+2*65536.
// On success: three counts, flags, then priority/other/non-lazy indices.
// Only that prefix is initialized. On failure no output is usable.
// Nonempty tags and output must be valid, live, disjoint allocations;
// output must be aligned, writable and exclusively accessible during the call.
// Empty tags may be NULL. No pointers or allocation ownership are retained.
uint32_t opcode_project_v1(
    const uint8_t *tags,
    uint32_t tag_count,
    uint32_t *output,
    uint32_t output_capacity);

#cgo amd64 LDFLAGS: ${SRCDIR}/../../rust/target/x86_64-unknown-linux-gnu/release/libopcode_bridge.a -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc
#cgo arm64 LDFLAGS: ${SRCDIR}/../../rust/target/aarch64-unknown-linux-gnu/release/libopcode_bridge.a -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"unsafe"
)

func EvaluateFFI(ctx context.Context, tags []Tag, requestID uint64) (Projection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Projection{}, err
	}
	request, err := EncodePlan(requestID, tags)
	if err != nil {
		return Projection{}, err
	}
	// Groups partition n inputs and the non-lazy list is at most n positions.
	// This is the exact maximum frame size for this request count.
	output := make([]byte, HeaderSize+16+8*len(tags))
	var written C.uint32_t
	status := C.opcode_eval_v1(
		(*C.uint8_t)(unsafe.Pointer(&request[0])),
		C.uint32_t(len(request)),
		(*C.uint8_t)(unsafe.Pointer(&output[0])),
		C.uint32_t(len(output)),
		(*C.uint32_t)(unsafe.Pointer(&written)),
	)
	runtime.KeepAlive(request)
	runtime.KeepAlive(output)
	runtime.KeepAlive(&written)
	if err := ctx.Err(); err != nil {
		return Projection{}, err
	}
	if status != 0 {
		return Projection{}, &RemoteError{RequestID: requestID, Code: ErrorCode(status)}
	}
	if written == 0 || uint64(written) > uint64(len(output)) {
		return Projection{}, fmt.Errorf("opcode FFI returned invalid byte count %d", uint32(written))
	}
	return DecodeResult(requestID, tags, output[:int(written)])
}

// EvaluateDirectFFI uses bounded caller-owned native-value storage. Like the
// framed adapter it checks cancellation before/after the synchronous call;
// native execution cannot be interrupted. requestID is caller-local diagnostic
// correlation only; it does not cross this ABI.
func EvaluateDirectFFI(ctx context.Context, tags []Tag, requestID uint64) (Projection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Projection{}, err
	}
	if requestID == 0 {
		return Projection{}, ErrInvalidRequestID
	}
	if len(tags) > MaxOpcodes {
		return Projection{}, ErrTooManyOpcodes
	}
	for index, tag := range tags {
		if !tag.valid() {
			return Projection{}, fmt.Errorf("%w: tag %d at position %d", ErrUnknownOpcode, tag, index)
		}
	}
	// Groups partition n positions and the non-lazy list has at most n entries.
	// Tag's underlying uint8 and Go uint32 match the fixed-width C parameters;
	// both buffers contain only scalars and remain live until native return.
	output := make([]uint32, 4+2*len(tags))
	status := C.opcode_project_v1(
		(*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(tags))),
		C.uint32_t(len(tags)),
		(*C.uint32_t)(unsafe.Pointer(unsafe.SliceData(output))),
		C.uint32_t(len(output)),
	)
	runtime.KeepAlive(tags)
	runtime.KeepAlive(output)
	if err := ctx.Err(); err != nil {
		return Projection{}, err
	}
	if status != 0 {
		return Projection{}, &RemoteError{RequestID: requestID, Code: ErrorCode(status)}
	}
	priority, other, nonLazy, flags := uint64(output[0]), uint64(output[1]), uint64(output[2]), output[3]
	if priority > uint64(len(tags)) || other > uint64(len(tags)) || nonLazy > uint64(len(tags)) ||
		priority+other != uint64(len(tags)) || 4+priority+other+nonLazy > uint64(len(output)) ||
		flags & ^uint32(ResultFlagHistory|ResultFlagHasPlan) != 0 {
		return Projection{}, fmt.Errorf("opcode direct FFI returned invalid counts or flags")
	}
	pEnd, oEnd, nEnd := 4+int(priority), 4+int(priority+other), 4+int(priority+other+nonLazy)
	projection := Projection{
		PriorityIndices: output[4:pEnd:pEnd],
		OtherIndices:    output[pEnd:oEnd:oEnd],
		NonLazyIndices:  output[oEnd:nEnd:nEnd],
		HistoryGroup:    flags&ResultFlagHistory != 0,
		HasPlanOp:       flags&ResultFlagHasPlan != 0,
	}
	if err := ValidateProjection(tags, projection); err != nil {
		return Projection{}, err
	}
	return projection, nil
}
