//go:build linux && response_continuation_spike && cgo

// Package responsecontinuationspike is a test-gated native-session experiment.
// It is not an execution selector or a production host.
package responsecontinuationspike

/*
#include <stdint.h>
typedef struct {
 uint64_t session;
 uint64_t output;
 const uint8_t *data;
 uint32_t len;
} response_continuation_output;
uint32_t response_continuation_start_v1(const uint8_t*, uint32_t, response_continuation_output*);
uint32_t response_continuation_resume_v1(uint64_t, const uint8_t*, uint32_t, response_continuation_output*);
uint32_t response_continuation_output_release_v1(uint64_t);
uint32_t response_continuation_close_v1(uint64_t);
#cgo amd64 LDFLAGS: ${SRCDIR}/../../rust/target/x86_64-unknown-linux-gnu/release/libresponse_continuation_bridge.a -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc
#cgo arm64 LDFLAGS: ${SRCDIR}/../../rust/target/aarch64-unknown-linux-gnu/release/libresponse_continuation_bridge.a -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

const MaxBytes = 16 * 1024 * 1024

type Key struct {
	Generation uint64          `json:"generation"`
	Owner      json.RawMessage `json:"owner"`
	Sequence   uint64          `json:"sequence"`
}
type Request struct {
	Key    Key             `json:"key"`
	Effect json.RawMessage `json:"effect"`
}
type Fault struct {
	Handle        uint64 `json:"handle"`
	Duplicate     bool   `json:"duplicate"`
	Idempotent    bool   `json:"idempotent"`
	StateOverflow bool   `json:"state_overflow"`
	AlwaysRetry   bool   `json:"always_retry"`
}
type Disposition struct {
	Fault   Fault           `json:"fault"`
	Context string          `json:"context"`
	Policy  json.RawMessage `json:"policy"`
}
type Progress struct {
	Ready      []Request    `json:"ready"`
	Done       bool         `json:"done"`
	Completion *Disposition `json:"completion"`
}

// Session retains only an integer native handle. All input is copied by Rust;
// all output is copied to Go before Rust's allocation is released.
type Session struct {
	mu       sync.Mutex
	handle   uint64
	closed   bool
	poisoned error
}

func takeOutput(out C.response_continuation_output) (progress Progress, result error) {
	if out.output == 0 {
		return Progress{}, errors.New("native output has no allocation handle")
	}
	defer func() {
		if status := C.response_continuation_output_release_v1(out.output); status != 0 {
			progress = Progress{}
			result = fmt.Errorf("native output release status %d", uint32(status))
		}
	}()
	if out.data == nil || out.len == 0 || out.len > MaxBytes {
		return Progress{}, errors.New("native output has invalid byte bounds")
	}
	bytes := C.GoBytes(unsafe.Pointer(out.data), C.int(out.len))
	var p Progress
	if err := json.Unmarshal(bytes, &p); err != nil {
		return Progress{}, fmt.Errorf("native output: %w", err)
	}
	if len(p.Ready) > 65536 || p.Done && len(p.Ready) != 0 {
		return Progress{}, errors.New("native output has invalid progress")
	}
	return p, nil
}
func Start(input []byte) (*Session, Progress, error) {
	if len(input) == 0 || len(input) > MaxBytes {
		return nil, Progress{}, errors.New("native input exceeds bounds")
	}
	var out C.response_continuation_output
	status := C.response_continuation_start_v1((*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(input))), C.uint32_t(len(input)), &out)
	runtime.KeepAlive(input)
	if status != 0 {
		return nil, Progress{}, fmt.Errorf("native start status %d", uint32(status))
	}
	s := &Session{handle: uint64(out.session)}
	p, err := takeOutput(out)
	if err != nil {
		_ = s.Close()
		return nil, Progress{}, err
	}
	return s, p, nil
}

// Resume returns successful progress exactly once even if the caller's context
// is cancelled during the synchronous native call. The effect owner controls
// cancellation and draining; a post-call cancellation check must not discard a
// consumed key's newly ready set. Any protocol/native failure poisons this host
// session, so a caller cannot replay an effect or fall back to another runner.
func (s *Session) Resume(key Key, observation any) (Progress, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Progress{}, errors.New("native session closed")
	}
	if s.poisoned != nil {
		return Progress{}, s.poisoned
	}
	input, err := json.Marshal(struct {
		Key         Key `json:"key"`
		Observation any `json:"observation"`
	}{key, observation})
	if err == nil && (len(input) == 0 || len(input) > MaxBytes) {
		err = errors.New("native observation exceeds bounds")
	}
	if err == nil {
		var out C.response_continuation_output
		status := C.response_continuation_resume_v1(C.uint64_t(s.handle), (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(input))), C.uint32_t(len(input)), &out)
		runtime.KeepAlive(input)
		if status != 0 {
			err = fmt.Errorf("native resume status %d", uint32(status))
		} else {
			var p Progress
			p, err = takeOutput(out)
			if err == nil {
				return p, nil
			}
		}
	}
	s.poisoned = fmt.Errorf("native session poisoned: %w", err)
	return Progress{}, s.poisoned
}
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if status := C.response_continuation_close_v1(C.uint64_t(s.handle)); status != 0 {
		return fmt.Errorf("native close status %d", uint32(status))
	}
	return nil
}
