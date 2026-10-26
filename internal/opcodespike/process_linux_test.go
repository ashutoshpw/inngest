//go:build linux && opcode_bridge_spike

package opcodespike

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if mode := os.Getenv("OPCODE_SPIKE_HELPER"); mode != "" {
		if mode == "pipe-holder" {
			appendMarker(os.Getenv("OPCODE_SPIKE_MARKER"), fmt.Sprintf("holder-pid:%d", os.Getpid()))
			time.Sleep(30 * time.Second)
			os.Exit(0)
		}
		os.Exit(runAdversarialWorker(mode))
	}
	if err := enableChildSubreaper(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "enable test child subreaper:", err)
		os.Exit(2)
	}
	os.Exit(m.Run())
}

func enableChildSubreaper() error {
	const prSetChildSubreaper = 36
	_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0, 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func runAdversarialWorker(mode string) int {
	marker := os.Getenv("OPCODE_SPIKE_MARKER")
	if marker != "" {
		appendMarker(marker, "start")
		appendMarker(marker, fmt.Sprintf("pid:%d", os.Getpid()))
	}

	hello, err := ReadFrame(os.Stdin)
	if err != nil {
		return 2
	}
	frame, err := DecodeFrame(hello)
	if err != nil || frame.Kind != KindHello || frame.RequestID != 0 || !equalBytes(frame.Payload, handshakePayload[:]) {
		return 3
	}
	if mode == "startup-stall" {
		time.Sleep(5 * time.Second)
		return 0
	}
	if mode == "restart-startup-stall" && markerCount(marker, "start") > 1 {
		time.Sleep(30 * time.Second)
		return 0
	}
	if mode == "startup-truncated" {
		_, _ = os.Stdout.Write([]byte{'I', 'O', 'P'})
		return 0
	}
	readyPayload := append([]byte(nil), handshakePayload[:]...)
	if mode == "startup-mismatch" ||
		((mode == "restart-start-fail" || mode == "partial-pool-start-fail") && markerCount(marker, "start") > 1) {
		readyPayload[0] ^= 1
	}
	ready, _ := EncodeFrame(KindReady, 0, 0, readyPayload)
	if WriteFull(os.Stdout, ready) != nil {
		return 4
	}
	if marker != "" {
		appendMarker(marker, "ready")
	}
	if mode == "stderr" {
		_, _ = os.Stderr.Write(bytesRepeated('x', 96*1024))
	}
	if mode == "exit-after-ready" {
		return 0
	}

	plan, err := ReadFrame(os.Stdin)
	if err != nil {
		return 5
	}
	request, err := DecodeFrame(plan)
	if err != nil {
		return 6
	}
	if request.Kind == KindShutdown {
		if mode == "escaped-stderr-close" {
			if startEscapedStderrHolder(marker) != nil {
				return 8
			}
		}
		if mode == "shutdown-timeout" {
			time.Sleep(5 * time.Second)
			return 0
		}
		if mode == "shutdown-missing-bye" {
			return 0
		}
		requestID := uint64(0)
		switch mode {
		case "shutdown-immediate-bye-exit":
			// WriteFull below flushes the frame to the pipe before this process exits.
		case "shutdown-wrong-request":
			requestID = 1
		}
		bye, _ := EncodeFrame(KindBye, requestID, 0, nil)
		if mode == "shutdown-malformed-bye" {
			binary.LittleEndian.PutUint32(bye[20:24], 1)
		}
		if WriteFull(os.Stdout, bye) != nil {
			return 7
		}
		if mode == "shutdown-nonzero-exit" {
			return 9
		}
		return 0
	}
	if request.Kind != KindPlan {
		return 6
	}
	if marker != "" {
		appendMarker(marker, "plan")
	}
	switch mode {
	case "stall", "restart-start-fail", "restart-startup-stall", "close-timer", "cycle-fail":
		time.Sleep(5 * time.Second)
		return 0
	case "exit-after-plan":
		return 0
	case "truncate":
		_, _ = os.Stdout.Write([]byte{'I', 'O', 'P', 'C', 1})
		return 0
	case "oversized":
		bad := make([]byte, HeaderSize)
		copy(bad[:4], frameMagic[:])
		binary.LittleEndian.PutUint16(bad[4:6], ProtocolVersion)
		binary.LittleEndian.PutUint16(bad[6:8], uint16(KindResult))
		binary.LittleEndian.PutUint64(bad[8:16], request.RequestID)
		binary.LittleEndian.PutUint32(bad[16:20], MaxResultPayloadSize+1)
		_, _ = os.Stdout.Write(bad)
		return 0
	case "wrong-version":
		bad, _ := EncodeFrame(KindResult, request.RequestID, 0, make([]byte, 16))
		binary.LittleEndian.PutUint16(bad[4:6], ProtocolVersion+1)
		_, _ = os.Stdout.Write(bad)
		return 0
	case "unknown-kind":
		bad, _ := EncodeFrame(KindBye, 0, 0, nil)
		binary.LittleEndian.PutUint16(bad[6:8], 99)
		_, _ = os.Stdout.Write(bad)
		return 0
	case "reserved-flags":
		payload := make([]byte, 16+8)
		binary.LittleEndian.PutUint32(payload[0:4], 1)
		binary.LittleEndian.PutUint32(payload[4:8], 1)
		binary.LittleEndian.PutUint32(payload[8:12], 0)
		binary.LittleEndian.PutUint32(payload[12:16], 4)
		binary.LittleEndian.PutUint32(payload[16:20], 0)
		binary.LittleEndian.PutUint32(payload[20:24], 1)
		bad, _ := EncodeFrame(KindResult, request.RequestID, 0, payload)
		_, _ = os.Stdout.Write(bad)
		return 0
	case "remote-error":
		bad, _ := EncodeFrame(KindError, request.RequestID, CodeInvalidPlan, nil)
		_, _ = os.Stdout.Write(bad)
		return 0
	case "wrong-id":
		bad := invalidResult(request.RequestID + 1)
		_, _ = os.Stdout.Write(bad)
		return 0
	case "invalid-index", "trailing-result":
		bad := invalidResult(request.RequestID)
		if mode == "trailing-result" {
			payload := append(append([]byte(nil), bad[HeaderSize:]...), 0)
			bad, _ = EncodeFrame(KindResult, request.RequestID, 0, payload)
		}
		_, _ = os.Stdout.Write(bad)
		return 0
	case "stderr":
		bad, _ := EncodeFrame(KindError, request.RequestID, CodeInvalidPlan, nil)
		_, _ = os.Stdout.Write(bad)
		return 0
	case "escaped-stderr-error":
		if startEscapedStderrHolder(marker) != nil {
			return 8
		}
		bad, _ := EncodeFrame(KindError, request.RequestID, CodeInvalidPlan, nil)
		_, _ = os.Stdout.Write(bad)
		return 0
	default:
		bad := invalidResult(request.RequestID)
		_, _ = os.Stdout.Write(bad)
		return 0
	}
}

func startEscapedStderrHolder(marker string) error {
	command := exec.Command(os.Args[0], "-test.run=^$")
	command.Env = replaceEnv(os.Environ(), "OPCODE_SPIKE_HELPER", "pipe-holder")
	command.Env = replaceEnv(command.Env, "OPCODE_SPIKE_MARKER", marker)
	command.Stdin = strings.NewReader("")
	command.Stdout = io.Discard
	command.Stderr = os.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		return err
	}
	appendMarker(marker, fmt.Sprintf("escaped-pid:%d", command.Process.Pid))
	return command.Process.Release()
}

func replaceEnv(environment []string, key, value string) []string {
	prefix := key + "="
	filtered := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return append(filtered, prefix+value)
}

func invalidResult(requestID uint64) []byte {
	payload := make([]byte, 16+8)
	binary.LittleEndian.PutUint32(payload[0:4], 0)
	binary.LittleEndian.PutUint32(payload[4:8], 1)
	binary.LittleEndian.PutUint32(payload[8:12], 1)
	binary.LittleEndian.PutUint32(payload[12:16], 0)
	binary.LittleEndian.PutUint32(payload[16:20], 1)
	binary.LittleEndian.PutUint32(payload[20:24], 1)
	// This is transport fault injection only; it does not implement semantics.
	frame, _ := EncodeFrame(KindResult, requestID, 0, payload)
	return frame
}

func bytesRepeated(value byte, length int) []byte {
	output := make([]byte, length)
	for index := range output {
		output[index] = value
	}
	return output
}

func appendMarker(path, value string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintln(file, value)
	_ = file.Close()
}

func markerCount(path, value string) int {
	contents, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	count := 0
	for _, line := range strings.Split(string(contents), "\n") {
		if line == value {
			count++
		}
	}
	return count
}

func helperConfig(t *testing.T, mode, marker string) WorkerConfig {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	config := WorkerConfig{
		BinaryPath:      executable,
		Args:            []string{"-test.run=^$"},
		Env:             []string{"OPCODE_SPIKE_HELPER=" + mode, "GORACE=atexit_sleep_ms=0"},
		StartupTimeout:  500 * time.Millisecond,
		RequestTimeout:  300 * time.Millisecond,
		ShutdownTimeout: 100 * time.Millisecond,
		CleanupTimeout:  1 * time.Second,
	}
	if marker != "" {
		config.Env = append(config.Env, "OPCODE_SPIKE_MARKER="+marker)
	}
	return config
}

func TestWorkerCloseConsumesByeWhenWaitIsAlreadyComplete(t *testing.T) {
	stdinReader, stdin, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = closeFiles(stdinReader, stdin)
		t.Fatal(err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = closeFiles(stdinReader, stdin, stdout, stdoutWriter)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = closeFiles(stdinReader, stdoutWriter, stderrWriter) })

	waitDone := make(chan struct{})
	close(waitDone)
	stderrDone := make(chan struct{})
	close(stderrDone)
	worker := &Worker{
		stdin:        stdin,
		stdout:       stdout,
		stderrReader: stderrReader,
		stderr:       &limitedStderr{},
		stderrDone:   stderrDone,
		waitDone:     waitDone,
		config: WorkerConfig{
			ShutdownTimeout: time.Second,
			CleanupTimeout:  time.Second,
		},
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- worker.Close() }()
	request, err := ReadFrame(stdinReader)
	if err != nil {
		t.Fatalf("read shutdown request: %v", err)
	}
	parsed, err := DecodeFrame(request)
	if err != nil || parsed.Kind != KindShutdown {
		t.Fatalf("request = %+v, decode error = %v; want shutdown", parsed, err)
	}
	bye, _ := EncodeFrame(KindBye, 0, 0, nil)
	// Hold the response until the request is visible so the exit notification is
	// ready while the shutdown exchange has not consumed BYE yet.
	_ = WriteFull(stdoutWriter, bye)
	_ = closeFiles(stdoutWriter)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close rejected BYE after process Wait completed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after consuming the buffered BYE")
	}
	if err := waitForJoin(worker.ioDone, time.Second); err != nil {
		t.Fatalf("shutdown request I/O did not join: %v", err)
	}
}

func TestWorkerCloseValidatesShutdownAckAndExit(t *testing.T) {
	tests := []struct {
		name            string
		wantErr         bool
		wantErrorIs     error
		wantErrorText   string
		shutdownTimeout time.Duration
	}{
		{name: "shutdown-immediate-bye-exit"},
		{name: "shutdown-malformed-bye", wantErr: true, wantErrorText: "invalid opcode frame status or payload length"},
		{name: "shutdown-wrong-request", wantErr: true, wantErrorText: "invalid opcode worker shutdown acknowledgement"},
		{name: "shutdown-missing-bye", wantErr: true, wantErrorIs: ErrCleanEOF},
		{name: "shutdown-nonzero-exit", wantErr: true, wantErrorText: "exited unsuccessfully"},
		{name: "shutdown-timeout", wantErr: true, wantErrorIs: context.DeadlineExceeded, shutdownTimeout: 50 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := helperConfig(t, test.name, "")
			if test.shutdownTimeout > 0 {
				config.ShutdownTimeout = test.shutdownTimeout
			}
			worker, err := NewWorker(config)
			if err != nil {
				t.Fatal(err)
			}

			closeErr := worker.Close()
			if (closeErr != nil) != test.wantErr {
				t.Fatalf("Close error = %v, wantErr %v", closeErr, test.wantErr)
			}
			if test.wantErrorIs != nil && !errors.Is(closeErr, test.wantErrorIs) {
				t.Fatalf("Close error = %v, want errors.Is(_, %v)", closeErr, test.wantErrorIs)
			}
			if test.wantErrorText != "" && (closeErr == nil || !strings.Contains(closeErr.Error(), test.wantErrorText)) {
				t.Fatalf("Close error = %v, want text %q", closeErr, test.wantErrorText)
			}
			if err := waitForJoin(worker.waitDone, config.CleanupTimeout+time.Second); err != nil {
				t.Fatalf("worker process did not join: %v", err)
			}
			if err := waitForJoin(worker.stderrDone, config.CleanupTimeout+time.Second); err != nil {
				t.Fatalf("worker stderr drainer did not join: %v", err)
			}
		})
	}
}

func TestProcessRejectsFaultyWorkersAndMakesResultsUnusable(t *testing.T) {
	for _, mode := range []string{"truncate", "oversized", "wrong-version", "unknown-kind", "reserved-flags", "remote-error", "wrong-id", "invalid-index", "trailing-result", "exit-after-plan"} {
		t.Run(mode, func(t *testing.T) {
			worker, err := NewWorker(helperConfig(t, mode, ""))
			if err != nil {
				t.Fatal(err)
			}
			pid := worker.PID()
			projection, err := worker.Evaluate(context.Background(), []Tag{TagNone})
			if err == nil {
				t.Fatalf("faulty worker returned usable projection %+v", projection)
			}
			if !worker.Poisoned() {
				t.Fatal("protocol or worker failure did not poison the worker")
			}
			if !isZeroProjection(projection) {
				t.Fatalf("failed result was not discarded: %+v", projection)
			}
			if err := waitForJoin(worker.waitDone, time.Second); err != nil {
				t.Fatalf("worker wait owner did not reap PID %d: %v", pid, err)
			}
			if err := waitForJoin(worker.stderrDone, time.Second); err != nil {
				t.Fatalf("stderr drainer did not join for PID %d: %v", pid, err)
			}
			if err := worker.Close(); err != nil {
				t.Fatalf("idempotent poisoned close: %v", err)
			}
		})
	}
}

func TestStartupFailuresAreBoundedAndReaped(t *testing.T) {
	for _, mode := range []string{"startup-stall", "startup-truncated", "startup-mismatch"} {
		t.Run(mode, func(t *testing.T) {
			config := helperConfig(t, mode, "")
			config.StartupTimeout = 100 * time.Millisecond
			started := time.Now()
			worker, err := NewWorker(config)
			if err == nil {
				_ = worker.Close()
				t.Fatal("invalid startup handshake was accepted")
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("startup failure cleanup took %s", elapsed)
			}
		})
	}
}

func TestPoolRestartsAfterCleanupWithoutRetryingFailedRequest(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "worker-events.log")
	config := helperConfig(t, "stall", marker)
	config.RequestTimeout = time.Second
	pool, err := NewPool(context.Background(), PoolConfig{Worker: config, Size: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	oldWorker := pool.slots[0].worker
	oldPID := oldWorker.PID()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if projection, err := pool.Evaluate(ctx, []Tag{TagNone}); err == nil || !isZeroProjection(projection) {
		t.Fatalf("cancelled result = %+v, %v", projection, err)
	}
	waitForMarker(t, marker, "start", 2, 2*time.Second)
	if got := markerCount(marker, "plan"); got != 1 {
		t.Fatalf("failed request was retried: observed %d PLAN frames", got)
	}
	if err := waitForJoin(oldWorker.waitDone, time.Second); err != nil {
		t.Fatalf("old worker PID %d was not reaped: %v", oldPID, err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	for _, slot := range pool.slots {
		if err := waitForJoin(slot.worker.waitDone, time.Second); err != nil {
			t.Fatalf("replacement worker did not reap: %v", err)
		}
		if err := waitForJoin(slot.worker.stderrDone, time.Second); err != nil {
			t.Fatalf("replacement stderr drainer did not join: %v", err)
		}
	}
}

func TestPoolCapsRestartAttemptsAndCloseCancelsBackoff(t *testing.T) {
	t.Run("three attempts", func(t *testing.T) {
		marker := filepath.Join(t.TempDir(), "worker-events.log")
		pool, err := NewPool(context.Background(), PoolConfig{
			Worker: helperConfig(t, "restart-start-fail", marker), Size: 1, MaxQueued: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, _ = pool.Evaluate(ctx, []Tag{TagNone})
		cancel()
		waitForMarker(t, marker, "start", 4, 3*time.Second)
		if got := markerCount(marker, "start"); got != 4 {
			t.Fatalf("startup count = %d, want initial plus three restart attempts", got)
		}
		if got := markerCount(marker, "plan"); got != 1 {
			t.Fatalf("failed PLAN was retried: count %d", got)
		}
		if _, err := pool.Evaluate(context.Background(), []Tag{TagNone}); err == nil {
			t.Fatal("pool with exhausted worker slot remained usable")
		}
		if err := pool.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("close cancels timer", func(t *testing.T) {
		marker := filepath.Join(t.TempDir(), "worker-events.log")
		pool, err := NewPool(context.Background(), PoolConfig{
			Worker: helperConfig(t, "close-timer", marker), Size: 1, MaxQueued: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, _ = pool.Evaluate(ctx, []Tag{TagNone})
		cancel()
		if err := pool.Close(); err != nil {
			t.Fatal(err)
		}
		if got := markerCount(marker, "start"); got != 1 {
			t.Fatalf("pool close did not cancel restart backoff; starts=%d", got)
		}
	})
}

func TestPoolBoundsAdmissionAndQueuedCancellationDoesNotSend(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "worker-events.log")
	config := helperConfig(t, "stall", marker)
	config.RequestTimeout = time.Second
	pool, err := NewPool(context.Background(), PoolConfig{Worker: config, Size: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}

	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancelFirst()
	firstDone := make(chan error, 1)
	go func() {
		_, err := pool.Evaluate(firstCtx, []Tag{TagNone})
		firstDone <- err
	}()
	waitForMarker(t, marker, "plan", 1, time.Second)

	queuedCtx, cancelQueued := context.WithTimeout(context.Background(), 120*time.Millisecond)
	queuedDone := make(chan error, 1)
	go func() {
		_, err := pool.Evaluate(queuedCtx, []Tag{TagNone})
		queuedDone <- err
	}()
	waitForAdmission(t, pool, 2, time.Second)
	_, thirdErr := pool.Evaluate(context.Background(), []Tag{TagNone})
	if !errors.Is(thirdErr, ErrWorkerBusy) {
		t.Fatalf("third admission = %v, want bounded busy result", thirdErr)
	}
	if err := <-queuedDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued cancellation = %v", err)
	}
	cancelQueued()
	if err := <-firstDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("in-flight cancellation = %v", err)
	}
	if got := markerCount(marker, "plan"); got != 1 {
		t.Fatalf("queued cancellation sent PLAN; observed %d requests", got)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEscapedStderrHolderDoesNotBlockDirectWaitAndCloseRetainsFailure(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "worker-events.log")
	t.Cleanup(func() { cleanupEscapedHolders(t, marker) })
	config := helperConfig(t, "escaped-stderr-close", marker)
	config.CleanupTimeout = 300 * time.Millisecond
	worker, err := NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	workerPID := worker.PID()

	closeDone := make(chan error, 1)
	go func() { closeDone <- worker.Close() }()
	waitForMarker(t, marker, "start", 1, time.Second)
	waitForMarkerPrefix(t, marker, "holder-pid:", 1, time.Second)
	if err := waitForJoin(worker.waitDone, config.CleanupTimeout); err != nil {
		t.Fatalf("direct worker PID %d was not reaped while an escaped descendant retained stderr: %v", workerPID, err)
	}
	if isChannelClosed(worker.stderrDone) {
		t.Fatal("stderr drainer reached EOF while the escaped descendant still held the pipe")
	}

	select {
	case closeErr := <-closeDone:
		if !errors.Is(closeErr, errStderrReaderForced) {
			t.Fatalf("Close error = %v, want retained forced-stderr cleanup failure", closeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close exceeded the single cleanup deadline")
	}
	if err := waitForJoin(worker.stderrDone, time.Second); err != nil {
		t.Fatalf("forced stderr reader close did not join its drainer: %v", err)
	}
	if processExists(workerPID) {
		t.Fatalf("direct worker PID %d remains in /proc after Wait", workerPID)
	}
	if err := worker.Close(); !errors.Is(err, errStderrReaderForced) {
		t.Fatalf("idempotent Close = %v, want the original cleanup failure", err)
	}

}

func TestPoolCloseCancelsActiveAndQueuedEvaluations(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "worker-events.log")
	config := helperConfig(t, "stall", marker)
	config.RequestTimeout = 5 * time.Second
	pool, err := NewPool(context.Background(), PoolConfig{Worker: config, Size: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	workerPID := pool.PIDs()[0]

	type evaluation struct {
		projection Projection
		err        error
	}
	activeDone := make(chan evaluation, 1)
	go func() {
		projection, err := pool.Evaluate(context.Background(), []Tag{TagNone})
		activeDone <- evaluation{projection: projection, err: err}
	}()
	waitForMarker(t, marker, "plan", 1, time.Second)

	queuedDone := make(chan evaluation, 1)
	go func() {
		projection, err := pool.Evaluate(context.Background(), []Tag{TagNone})
		queuedDone <- evaluation{projection: projection, err: err}
	}()
	waitForAdmission(t, pool, 2, time.Second)

	closeDone := make(chan error, 1)
	go func() { closeDone <- pool.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("pool Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pool Close did not cancel and join active work")
	}

	for name, done := range map[string]<-chan evaluation{"active": activeDone, "queued": queuedDone} {
		select {
		case result := <-done:
			if !errors.Is(result.err, ErrPoolClosed) {
				t.Errorf("%s Evaluate error = %v, want ErrPoolClosed", name, result.err)
			}
			if !isZeroProjection(result.projection) {
				t.Errorf("%s Evaluate returned a usable result after Close: %+v", name, result.projection)
			}
		case <-time.After(time.Second):
			t.Errorf("%s Evaluate did not return after Close", name)
		}
	}
	if got := markerCount(marker, "plan"); got != 1 {
		t.Fatalf("queued call was replayed or sent after Close: observed %d PLAN frames", got)
	}
	if processExists(workerPID) {
		t.Fatalf("direct worker PID %d remains after pool Close", workerPID)
	}
}

func TestPoolCloseDuringReplacementStartupCancelsAndJoins(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "worker-events.log")
	config := helperConfig(t, "restart-startup-stall", marker)
	config.RequestTimeout = 100 * time.Millisecond
	config.StartupTimeout = 30 * time.Second
	config.CleanupTimeout = 300 * time.Millisecond
	pool, err := NewPool(context.Background(), PoolConfig{Worker: config, Size: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if projection, evalErr := pool.Evaluate(ctx, []Tag{TagNone}); evalErr == nil || !isZeroProjection(projection) {
		t.Fatalf("stalled worker result = %+v, %v", projection, evalErr)
	}
	waitForMarker(t, marker, "start", 2, 2*time.Second)

	started := time.Now()
	if err := pool.Close(); err != nil {
		t.Fatalf("pool Close during replacement startup: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close waited for the replacement startup timeout: %s", elapsed)
	}
	for _, pid := range markerPIDs(t, marker, "pid:") {
		if err := waitForProcessGone(pid, time.Second); err != nil {
			t.Errorf("worker PID %d survived Close: %v", pid, err)
		}
	}
}

func TestPartialPoolConstructionClosesEarlierWorkers(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "worker-events.log")
	config := helperConfig(t, "partial-pool-start-fail", marker)
	config.CleanupTimeout = 300 * time.Millisecond
	pool, err := NewPool(context.Background(), PoolConfig{Worker: config, Size: 2, MaxQueued: 1})
	if err == nil {
		_ = pool.Close()
		t.Fatal("pool accepted a failed second worker startup")
	}
	waitForMarker(t, marker, "start", 2, time.Second)
	for _, pid := range markerPIDs(t, marker, "pid:") {
		if err := waitForProcessGone(pid, time.Second); err != nil {
			t.Errorf("partially constructed worker PID %d survived NewPool failure: %v", pid, err)
		}
	}
}

func TestPoolReplacementBudgetSpansSuccessfulCyclesAndEntersCooldown(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "worker-events.log")
	config := helperConfig(t, "cycle-fail", marker)
	config.RequestTimeout = 100 * time.Millisecond
	config.CleanupTimeout = 300 * time.Millisecond
	pool, err := NewPool(context.Background(), PoolConfig{Worker: config, Size: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	for cycle := 1; cycle <= 4; cycle++ {
		projection, evalErr := evaluateAfterRestart(t, pool, marker, cycle)
		if evalErr == nil || !isZeroProjection(projection) {
			t.Fatalf("cycle %d result = %+v, %v, want failed request discarded", cycle, projection, evalErr)
		}
		if cycle < 4 {
			waitForMarker(t, marker, "ready", cycle+1, 2*time.Second)
		}
	}
	if got := markerCount(marker, "plan"); got != 4 {
		t.Fatalf("pool replayed failed requests: PLAN count=%d, want 4", got)
	}
	if got := markerCount(marker, "start"); got != 4 {
		t.Fatalf("starts=%d, want initial worker plus three replacements in the rolling window", got)
	}
	if _, err := pool.Evaluate(context.Background(), []Tag{TagNone}); !errors.Is(err, errPoolUnavailable) {
		t.Fatalf("Evaluate during exhausted-worker cooldown = %v, want unavailable", err)
	}

	// A fifth start would expose per-cycle accounting: each healthy replacement
	// must not reset the pool-wide rolling attempt window.
	noFifthStart := time.NewTimer(400 * time.Millisecond)
	defer noFifthStart.Stop()
	select {
	case <-noFifthStart.C:
		if got := markerCount(marker, "start"); got != 4 {
			t.Fatalf("replacement started during cooldown: starts=%d", got)
		}
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- pool.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pool Close did not cancel the replacement cooldown")
	}
}

func TestPoolDoesNotReplaceWorkerWhenCleanupCannotBeConfirmed(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "worker-events.log")
	t.Cleanup(func() { cleanupEscapedHolders(t, marker) })
	config := helperConfig(t, "escaped-stderr-error", marker)
	config.CleanupTimeout = 300 * time.Millisecond
	pool, err := NewPool(context.Background(), PoolConfig{Worker: config, Size: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	workerPID := pool.PIDs()[0]
	projection, evalErr := pool.Evaluate(context.Background(), []Tag{TagNone})
	if !isZeroProjection(projection) || !errors.Is(evalErr, errStderrReaderForced) {
		t.Fatalf("failed result = %+v, %v, want discarded result and cleanup failure", projection, evalErr)
	}
	if processExists(workerPID) {
		t.Fatalf("direct worker PID %d was not reaped", workerPID)
	}
	if got := markerCount(marker, "start"); got != 1 {
		t.Fatalf("pool replaced a worker before cleanup was confirmed: starts=%d", got)
	}
	if _, err := pool.Evaluate(context.Background(), []Tag{TagNone}); !errors.Is(err, errPoolUnavailable) {
		t.Fatalf("Evaluate after cleanup failure = %v, want unavailable", err)
	}
	closeErr := pool.Close()
	if !errors.Is(closeErr, errStderrReaderForced) {
		t.Fatalf("pool Close did not retain the worker cleanup failure: %v", closeErr)
	}
	if err := pool.Close(); !errors.Is(err, errStderrReaderForced) {
		t.Fatalf("idempotent pool Close = %v, want retained cleanup failure", err)
	}
}

func TestWorkerCloseDoesNotAccumulatePIDsFileDescriptorsOrGoroutines(t *testing.T) {
	config := helperConfig(t, "normal", "")
	config.StartupTimeout = 2 * time.Second
	warmup, err := NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := warmup.Close(); err != nil {
		t.Fatal(err)
	}
	baselineFDs := openFDCount(t)
	baselineGoroutines := runtime.NumGoroutine()
	for iteration := 0; iteration < 8; iteration++ {
		worker, err := NewWorker(config)
		if err != nil {
			t.Fatalf("iteration %d startup: %v", iteration, err)
		}
		pid := worker.PID()
		if err := worker.Close(); err != nil {
			t.Fatalf("iteration %d Close: %v", iteration, err)
		}
		for name, done := range map[string]<-chan struct{}{
			"process Wait": worker.waitDone,
			"stderr drain": worker.stderrDone,
			"request I/O":  worker.ioDone,
		} {
			if err := waitForJoin(done, time.Second); err != nil {
				t.Fatalf("iteration %d %s did not join: %v", iteration, name, err)
			}
		}
		if err := waitForProcessGone(pid, time.Second); err != nil {
			t.Fatalf("iteration %d worker PID %d: %v", iteration, pid, err)
		}
	}
	if err := waitForResources(baselineFDs, baselineGoroutines, 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestStderrDrainRetainsAtMost64KiB(t *testing.T) {
	worker, err := NewWorker(helperConfig(t, "stderr", ""))
	if err != nil {
		t.Fatal(err)
	}
	projection, err := worker.Evaluate(context.Background(), []Tag{TagNone})
	if err == nil || !isZeroProjection(projection) {
		t.Fatalf("fault helper result = %+v, %v", projection, err)
	}
	if got := len(worker.Stderr()); got > maxStderrBytes || got == 0 {
		t.Fatalf("stderr retained %d bytes, limit %d", got, maxStderrBytes)
	}
}

func isZeroProjection(projection Projection) bool {
	return len(projection.PriorityIndices) == 0 &&
		len(projection.OtherIndices) == 0 &&
		len(projection.NonLazyIndices) == 0 &&
		!projection.HistoryGroup &&
		!projection.HasPlanOp
}

func waitForMarker(t *testing.T, path, event string, count int, timeout time.Duration) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if markerCount(path, event) >= count {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			contents, _ := os.ReadFile(path)
			t.Fatalf("timed out waiting for %d %q events in %s; events=%q", count, event, path, contents)
		}
	}
}

func waitForMarkerPrefix(t *testing.T, path, prefix string, count int, timeout time.Duration) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if markerPrefixCount(path, prefix) >= count {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			contents, _ := os.ReadFile(path)
			t.Fatalf("timed out waiting for %d marker prefixes %q in %s; events=%q", count, prefix, path, contents)
		}
	}
}

func markerPrefixCount(path, prefix string) int {
	contents, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	count := 0
	for _, line := range strings.Split(string(contents), "\n") {
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}
	return count
}

func waitForAdmission(t *testing.T, pool *Pool, count int, timeout time.Duration) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if len(pool.admission) == count {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("timed out waiting for %d admitted calls; got %d", count, len(pool.admission))
		}
	}
}

func evaluateAfterRestart(t *testing.T, pool *Pool, marker string, expectedPlans int) (Projection, error) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		projection, err := pool.Evaluate(ctx, []Tag{TagNone})
		cancel()
		if markerCount(marker, "plan") >= expectedPlans {
			return projection, err
		}
		if !errors.Is(err, errPoolUnavailable) {
			t.Fatalf("Evaluate returned before PLAN %d: %v", expectedPlans, err)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("pool did not deliver PLAN %d after replacement became ready", expectedPlans)
		}
	}
}

func waitForJoin(done <-chan struct{}, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

func markerPIDs(t *testing.T, path, prefix string) []int {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read marker file %s: %v", path, err)
	}
	return parseMarkerPIDs(contents, prefix)
}

func parseMarkerPIDs(contents []byte, prefix string) []int {
	var pids []int
	for _, line := range strings.Split(string(contents), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimPrefix(line, prefix))
		if err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}

func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	_, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
	return err == nil || !errors.Is(err, os.ErrNotExist)
}

func waitForProcessGone(pid int, timeout time.Duration) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for processExists(pid) {
		select {
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("PID %d still exists after %s", pid, timeout)
		}
	}
	return nil
}

func killAndReap(pid int) error {
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("kill PID %d: %w", pid, err)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var status syscall.WaitStatus
		waited, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		if waited == pid {
			return nil
		}
		if err != nil && !errors.Is(err, syscall.EINTR) && !errors.Is(err, syscall.ECHILD) {
			return fmt.Errorf("wait for PID %d: %w", pid, err)
		}
		if errors.Is(err, syscall.ECHILD) && !processExists(pid) {
			return nil
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return fmt.Errorf("PID %d was not reaped by test subreaper", pid)
		}
	}
}

func cleanupEscapedHolders(t *testing.T, marker string) {
	t.Helper()
	contents, err := os.ReadFile(marker)
	if err != nil {
		return
	}
	for _, pid := range parseMarkerPIDs(contents, "escaped-pid:") {
		if err := killAndReap(pid); err != nil {
			t.Errorf("reap escaped stderr holder PID %d: %v", pid, err)
		}
	}
}

func openFDCount(t *testing.T) int {
	t.Helper()
	count, err := currentFDCount()
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func currentFDCount() (int, error) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

func waitForResources(wantFDs, wantGoroutines int, timeout time.Duration) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		runtime.GC()
		fdCount, err := currentFDCount()
		if err != nil {
			return err
		}
		if fdCount == wantFDs && runtime.NumGoroutine() <= wantGoroutines+2 {
			return nil
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("resources did not return to baseline: file descriptors %d (want %d), goroutines %d (baseline %d)", fdCount, wantFDs, runtime.NumGoroutine(), wantGoroutines)
		}
	}
}
