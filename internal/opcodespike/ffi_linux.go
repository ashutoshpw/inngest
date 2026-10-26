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
