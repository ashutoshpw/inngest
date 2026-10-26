//go:build linux && opcode_bridge_spike && opcode_bridge_ffi && cgo

package opcodespike

import (
	"context"
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
	projection, err := EvaluateFFI(ctx, []Tag{TagNone}, 1)
	if err != context.Canceled {
		t.Fatalf("cancelled context error = %v", err)
	}
	if len(projection.PriorityIndices) != 0 || len(projection.OtherIndices) != 0 || len(projection.NonLazyIndices) != 0 || projection.HistoryGroup || projection.HasPlanOp {
		t.Fatalf("cancelled FFI call returned usable projection: %+v", projection)
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
