//go:build linux && opcode_bridge_spike

package executor

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpcodeBridgeMeasurementResourcePeaks(t *testing.T) {
	// Host and worker peak at different times. Their independent peaks sum to
	// 380; the actual simultaneous aggregate peaks at 250.
	var phase atomic.Int32
	observedMiddle := make(chan struct{})
	var once sync.Once
	tracker, err := newOpcodeSpikeResourceTracker(func() (opcodeSpikeResourceSnapshot, error) {
		host, worker := uint64(100), uint64(80)
		switch phase.Load() {
		case 1:
			host, worker = 50, 200
			once.Do(func() { close(observedMiddle) })
		case 2:
			host, worker = 180, 20
		}
		return opcodeSpikeResourceSnapshot{Host: opcodeSpikeProcessResources{RSSBytes: host},
			Workers: []opcodeSpikeProcessResources{{PID: 42, RSSBytes: worker}}, WorkerRSSBytes: worker, AggregateRSSBytes: host + worker}, nil
	}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	phase.Store(1)
	select {
	case <-observedMiddle:
	case <-time.After(time.Second):
		_, _, _, _ = tracker.Stop()
		t.Fatal("resource sampler did not observe intermediate RSS")
	}
	phase.Store(2)
	initial, peaks, final, err := tracker.Stop()
	if err != nil {
		t.Fatal(err)
	}
	if initial.AggregateRSSBytes != 180 || final.AggregateRSSBytes != 200 || peaks.AggregateRSSBytes != 250 || peaks.HostRSSBytes != 180 || peaks.WorkerRSSBytes != 200 || peaks.Workers[0].RSSBytes != 200 {
		t.Fatalf("initial=%+v peaks=%+v final=%+v", initial, peaks, final)
	}
	if peaks.Observations < 3 || peaks.IntervalNanoseconds != int64(time.Millisecond) {
		t.Fatalf("sampling metadata = %+v", peaks)
	}
}

func TestOpcodeBridgeMeasurementSamplerFailure(t *testing.T) {
	failure := errors.New("counter read failed")
	var reads atomic.Int32
	tracker, err := newOpcodeSpikeResourceTracker(func() (opcodeSpikeResourceSnapshot, error) {
		if reads.Add(1) > 1 {
			return opcodeSpikeResourceSnapshot{}, failure
		}
		return opcodeSpikeResourceSnapshot{AggregateRSSBytes: 100}, nil
	}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-tracker.done:
	case <-time.After(time.Second):
		_, _, _, _ = tracker.Stop()
		t.Fatal("sampler did not stop on a read failure")
	}
	_, _, _, err = tracker.Stop()
	if !errors.Is(err, failure) {
		t.Fatalf("sampler failure lost: %v", err)
	}
}

func TestOpcodeBridgeMeasurementLatencySummary(t *testing.T) {
	// Unsorted tail-heavy literal durations distinguish empirical quantiles
	// from means, batch timings, and floor-index percentiles.
	result := summarizeOpcodeSpikeLatency("test", "test", 8,
		[]time.Duration{100 * time.Nanosecond, time.Nanosecond, 4 * time.Nanosecond, 2 * time.Nanosecond, 3 * time.Nanosecond},
		2*time.Second, 100, 180, 120)
	if result.P50Nanoseconds != 3 || result.P95Nanoseconds != 100 || result.P99Nanoseconds != 100 || result.ThroughputPerSec != 2.5 || result.Samples != 5 || result.GrowthRSSBytes != 80 || result.StartRSSBytes != 100 || result.FinalRSSBytes != 120 {
		t.Fatalf("latency summary = %+v", result)
	}
	if result := summarizeOpcodeSpikeLatency("test", "test", 1, []time.Duration{time.Nanosecond}, time.Second, 100, 100, 60); result.GrowthRSSBytes != 0 {
		t.Fatalf("release-only RSS wrapped growth: %+v", result)
	}
}

func TestOpcodeBridgeMeasurementLiveHostCounters(t *testing.T) {
	prepareOpcodeSpikeResourceBaseline(t)
	before := captureOpcodeSpikeResources(t, "baseline", nil, false)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); <-stop }()
	defer func() { close(stop); <-done }()
	during := captureOpcodeSpikeResources(t, "open", nil, false)
	if during.Host.PID != os.Getpid() || during.Host.RSSBytes == 0 || during.Host.ThreadCount < 1 || during.Host.FDCount != before.Host.FDCount+2 || *during.Host.GoGoroutines < *before.Host.GoGoroutines+1 {
		t.Fatalf("live pipe/goroutine counters before=%+v during=%+v", before, during)
	}
	if during.WorkerRSSBytes != 0 || during.AggregateRSSBytes != during.Host.RSSBytes {
		t.Fatalf("host-only RSS included another process: %+v", during)
	}
	if _, err := readOpcodeSpikeProcessResources(-1, false, false); err == nil {
		t.Fatal("invalid PID silently became zero counters")
	}
}

func TestOpcodeBridgeMeasurementChildCountersAndExit(t *testing.T) {
	// Exercise actual /proc child counters without depending on a Rust build.
	child := exec.Command("/bin/sleep", "10")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	pids := []int{child.Process.Pid}
	live := captureOpcodeSpikeResources(t, "ready", pids, false)
	worker := live.Workers[0]
	if worker.PID != child.Process.Pid || worker.State != "present" || worker.FDCount < 3 || worker.ThreadCount < 1 || worker.RSSBytes == 0 || worker.GoGoroutines != nil {
		t.Fatalf("live child counters = %+v", worker)
	}
	if live.WorkerRSSBytes != worker.RSSBytes || live.AggregateRSSBytes != live.Host.RSSBytes+worker.RSSBytes {
		t.Fatalf("child-only and host-plus-child totals = %+v", live)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("killed child exited successfully")
	}
	after := captureOpcodeSpikeResources(t, "after_close", pids, true)
	worker = after.Workers[0]
	if worker.State != "absent_expected_after_close" || worker.FDCount != 0 || worker.ThreadCount != 0 || worker.RSSBytes != 0 || worker.GoGoroutines != nil || after.WorkerRSSBytes != 0 {
		t.Fatalf("exited child has fabricated resources: %+v", after)
	}
	if _, err := readOpcodeSpikeResources(pids, false); err == nil {
		t.Fatal("unexpected worker exit silently became zero counters")
	}
}

func TestOpcodeBridgeMeasurementExactSelection(t *testing.T) {
	allowed := []string{"small1", "mixed8", "large1000"}
	for _, value := range []string{"small", " small1", "small1,", "small1,small1", "small1,mixed"} {
		if _, err := opcodeSpikeSelectNames(value, allowed); err == nil {
			t.Errorf("invalid exact selection %q accepted", value)
		}
	}
	got, err := opcodeSpikeSelectNames("large1000,small1", allowed)
	if err != nil || !reflect.DeepEqual(got, []string{"large1000", "small1"}) {
		t.Fatalf("selected order = %v, %v", got, err)
	}
}

func TestOpcodeBridgeMeasurementLaneChildSelection(t *testing.T) {
	// Exercise the campaign subprocess entry point with no process worker installed.
	t.Setenv("OPCODE_WORKER_BIN", filepath.Join(t.TempDir(), "missing-worker"))
	for _, lane := range opcodeSpikeMeasurementLanes() {
		if lane == "process_decoded" || lane == "process_full_driver_response_json" {
			pool, err := newOpcodeSpikeLanePool(lane, 1)
			if pool != nil {
				_ = pool.Close()
			}
			if err == nil {
				t.Fatalf("process lane %s ignored the missing worker", lane)
			}
			continue
		}
		t.Run(lane, func(t *testing.T) {
			for _, concurrency := range []int{1, 8} {
				result := runOpcodeSpikeMeasurementChild(t, lane, "duplicates", concurrency, 2)
				if result.WorkerProcesses != 0 || result.RSSScope != "isolated_host" || result.StartRSSBytes == 0 || result.StartupReadyNanoseconds != nil || result.FirstRequestNanoseconds == nil || *result.FirstRequestNanoseconds <= 0 {
					t.Fatalf("non-process lane startup/resource scope: %+v", result)
				}
				if result.Lane != lane || result.Concurrency != concurrency || result.Samples != 2 {
					t.Fatalf("child measured different lane or request count: %+v", result)
				}
				if len(result.Resources) != 5 || result.Resources[0].Stage != "baseline" || result.Resources[2].Stage != "warmup" || result.Resources[4].Stage != "after_close" || result.Resources[4].Host.GoGoroutines == nil || result.RSSPeaks == nil || result.RSSPeaks.IntervalNanoseconds != int64(10*time.Millisecond) {
					t.Fatalf("missing staged resource measurements: %+v", result)
				}
			}
		})
	}
}
