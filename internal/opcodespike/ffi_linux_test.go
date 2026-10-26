//go:build linux && opcode_bridge_spike && opcode_bridge_ffi && cgo

package opcodespike

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestFFIEntryDisposableProbe(t *testing.T) {
	if os.Getenv("OPCODE_SPIKE_FFI_PROBE_CHILD") == "1" {
		projection, err := EvaluateFFI(context.Background(), []Tag{TagWaitForEvent, TagStepRun}, 1)
		if err != nil {
			t.Fatalf("bounded child FFI call: %v", err)
		}
		if len(projection.PriorityIndices) != 1 || len(projection.OtherIndices) != 1 {
			t.Fatalf("unexpected child FFI projection: %+v", projection)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, mustTestExecutable(t), "-test.run=^TestFFIEntryDisposableProbe$")
	command.Env = append(os.Environ(), "OPCODE_SPIKE_FFI_PROBE_CHILD=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("disposable FFI probe failed or exceeded its bound: %v\n%s", err, output)
	}
}

func TestFFIContextRejectsBeforeEnteringRust(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, adapter := range []struct {
		name     string
		evaluate func(context.Context, []Tag, uint64) (Projection, error)
	}{{"framed", EvaluateFFI}, {"direct", EvaluateDirectFFI}} {
		t.Run(adapter.name, func(t *testing.T) {
			projection, err := adapter.evaluate(ctx, []Tag{TagNone}, 1)
			if err != context.Canceled {
				t.Fatalf("cancelled context error = %v", err)
			}
			if len(projection.PriorityIndices) != 0 || len(projection.OtherIndices) != 0 || len(projection.NonLazyIndices) != 0 || projection.HistoryGroup || projection.HasPlanOp {
				t.Fatalf("cancelled FFI call returned usable projection: %+v", projection)
			}
		})
	}
}

func TestDirectFFIInputBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		tags []Tag
		id   uint64
		want error
	}{
		{"zero ID", []Tag{TagNone}, 0, ErrInvalidRequestID},
		{"unknown tag", []Tag{TagNone, 255}, 1, ErrUnknownOpcode},
		{"too many tags", make([]Tag, MaxOpcodes+1), 1, ErrTooManyOpcodes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := EvaluateDirectFFI(nil, tc.tags, tc.id)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if len(got.PriorityIndices)+len(got.OtherIndices)+len(got.NonLazyIndices) != 0 || got.HistoryGroup || got.HasPlanOp {
				t.Fatalf("rejected call returned usable output: %+v", got)
			}
		})
	}
	// Exercise the real adapter at its largest allocation/index boundary, including
	// the nil-context convention, rather than only a Rust-side capacity test.
	tags := make([]Tag, MaxOpcodes)
	for index := range tags {
		tags[index] = TagWaitForEvent
	}
	got, err := EvaluateDirectFFI(nil, tags, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.PriorityIndices) != MaxOpcodes || len(got.OtherIndices) != 0 || len(got.NonLazyIndices) != MaxOpcodes || !got.HistoryGroup || !got.HasPlanOp {
		t.Fatalf("maximum input returned wrong counts/flags")
	}
	for index := range tags {
		if got.PriorityIndices[index] != uint32(index) || got.NonLazyIndices[index] != uint32(index) {
			t.Fatalf("lost original position %d", index)
		}
	}
}

func mustTestExecutable(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return path
}
