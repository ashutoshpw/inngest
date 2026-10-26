//go:build linux && opcode_bridge_spike

package executor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

type opcodeSpikeProcessResources struct {
	PID          int    `json:"pid"`
	State        string `json:"state"`
	FDCount      int    `json:"fdCount,omitempty"`
	ThreadCount  int    `json:"threadCount,omitempty"`
	RSSBytes     uint64 `json:"rssBytes,omitempty"`
	GoGoroutines *int   `json:"goGoroutines,omitempty"`
}

type opcodeSpikeResourceSnapshot struct {
	Stage             string                        `json:"stage"`
	Host              opcodeSpikeProcessResources   `json:"host"`
	Workers           []opcodeSpikeProcessResources `json:"workers"`
	WorkerRSSBytes    uint64                        `json:"workerRSSBytes"`
	AggregateRSSBytes uint64                        `json:"aggregateRSSBytes"`
}

type opcodeSpikeWorkerRSSPeak struct {
	PID      int    `json:"pid"`
	RSSBytes uint64 `json:"rssBytes"`
}

type opcodeSpikeRSSPeaks struct {
	IntervalNanoseconds int64                      `json:"intervalNanoseconds"`
	Observations        int                        `json:"observations"`
	HostRSSBytes        uint64                     `json:"hostRSSBytes"`
	WorkerRSSBytes      uint64                     `json:"workerRSSBytes"`
	AggregateRSSBytes   uint64                     `json:"aggregateRSSBytes"`
	Workers             []opcodeSpikeWorkerRSSPeak `json:"workers"`
}

func readOpcodeSpikeProcessResources(pid int, host, expectAbsent bool) (opcodeSpikeProcessResources, error) {
	result := opcodeSpikeProcessResources{PID: pid, State: "present"}
	if pid <= 0 {
		return result, fmt.Errorf("invalid resource PID %d", pid)
	}
	proc := filepath.Join("/proc", strconv.Itoa(pid))
	status, err := os.ReadFile(filepath.Join(proc, "status"))
	if err != nil {
		if expectAbsent && errors.Is(err, os.ErrNotExist) {
			result.State = "absent_expected_after_close"
			return result, nil
		}
		return result, fmt.Errorf("pid %d status: %w", pid, err)
	}
	for _, line := range strings.Split(string(status), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "VmRSS:":
			kib, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil || len(fields) != 3 || fields[2] != "kB" {
				return result, fmt.Errorf("pid %d invalid VmRSS: %q", pid, line)
			}
			result.RSSBytes = kib * 1024
		case "Threads:":
			threads, err := strconv.Atoi(fields[1])
			if err != nil || threads <= 0 {
				return result, fmt.Errorf("pid %d invalid Threads: %q", pid, line)
			}
			result.ThreadCount = threads
		}
	}
	if result.RSSBytes == 0 || result.ThreadCount == 0 {
		return result, fmt.Errorf("pid %d missing live RSS/thread counters", pid)
	}
	fds, err := os.ReadDir(filepath.Join(proc, "fd"))
	if err != nil {
		return result, fmt.Errorf("pid %d file descriptors: %w", pid, err)
	}
	result.FDCount = len(fds)
	if host {
		goroutines := runtime.NumGoroutine()
		result.GoGoroutines = &goroutines
	}
	return result, nil
}

// One observation reads host and child counters in sequence. Totals are sums
// from this observation, never sums of the independently sampled process peaks.
func readOpcodeSpikeResources(pids []int, afterClose bool) (opcodeSpikeResourceSnapshot, error) {
	result := opcodeSpikeResourceSnapshot{Workers: make([]opcodeSpikeProcessResources, 0, len(pids))}
	host, err := readOpcodeSpikeProcessResources(os.Getpid(), true, false)
	if err != nil {
		return result, err
	}
	result.Host = host
	result.AggregateRSSBytes = host.RSSBytes
	seen := map[int]bool{host.PID: true}
	for _, pid := range pids {
		if seen[pid] {
			return result, fmt.Errorf("duplicate/host worker PID %d", pid)
		}
		seen[pid] = true
		worker, err := readOpcodeSpikeProcessResources(pid, false, afterClose)
		if err != nil {
			return result, err
		}
		if afterClose && worker.State == "present" {
			return result, fmt.Errorf("worker pid %d still present after close", pid)
		}
		result.Workers = append(result.Workers, worker)
		result.WorkerRSSBytes += worker.RSSBytes
	}
	result.AggregateRSSBytes += result.WorkerRSSBytes
	return result, nil
}

func captureOpcodeSpikeResources(t testing.TB, stage string, pids []int, afterClose bool) opcodeSpikeResourceSnapshot {
	t.Helper()
	result, err := readOpcodeSpikeResources(pids, afterClose)
	if err != nil {
		t.Fatalf("resources %s: %v", stage, err)
	}
	result.Stage = stage
	return result
}

func prepareOpcodeSpikeResourceBaseline(t testing.TB) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(r.Close(), w.Close()); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
}

func closedOpcodeSpikeResources(t testing.TB, baseline opcodeSpikeResourceSnapshot, pids []int) opcodeSpikeResourceSnapshot {
	t.Helper()
	// Pool.Close joins worker I/O. Allow scheduler bookkeeping to settle before
	// comparing host descriptors/goroutines, with a bounded failure path.
	deadline := time.Now().Add(time.Second)
	for {
		after := captureOpcodeSpikeResources(t, "after_close", pids, true)
		if after.Host.FDCount <= baseline.Host.FDCount && *after.Host.GoGoroutines <= *baseline.Host.GoGoroutines {
			return after
		}
		if time.Now().After(deadline) {
			t.Fatalf("resources after_close: host descriptors/goroutines did not return to baseline: before=%+v after=%+v", baseline.Host, after.Host)
		}
		time.Sleep(time.Millisecond)
	}
}

type opcodeSpikeResourceTracker struct {
	read    func() (opcodeSpikeResourceSnapshot, error)
	initial opcodeSpikeResourceSnapshot
	peaks   opcodeSpikeRSSPeaks
	err     error
	stop    chan struct{}
	done    chan struct{}
}

func newOpcodeSpikeResourceTracker(read func() (opcodeSpikeResourceSnapshot, error), interval time.Duration) (*opcodeSpikeResourceTracker, error) {
	initial, err := read()
	if err != nil {
		return nil, err
	}
	tracker := &opcodeSpikeResourceTracker{read: read, initial: initial, stop: make(chan struct{}), done: make(chan struct{})}
	tracker.peaks.IntervalNanoseconds = int64(interval)
	tracker.observe(initial)
	go func() {
		defer close(tracker.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if _, err := tracker.sample(); err != nil {
					tracker.err = err
					return
				}
			case <-tracker.stop:
				return
			}
		}
	}()
	return tracker, nil
}

func (tracker *opcodeSpikeResourceTracker) observe(snapshot opcodeSpikeResourceSnapshot) {
	tracker.peaks.Observations++
	tracker.peaks.HostRSSBytes = max(tracker.peaks.HostRSSBytes, snapshot.Host.RSSBytes)
	tracker.peaks.WorkerRSSBytes = max(tracker.peaks.WorkerRSSBytes, snapshot.WorkerRSSBytes)
	tracker.peaks.AggregateRSSBytes = max(tracker.peaks.AggregateRSSBytes, snapshot.AggregateRSSBytes)
	for _, worker := range snapshot.Workers {
		found := false
		for i := range tracker.peaks.Workers {
			if tracker.peaks.Workers[i].PID == worker.PID {
				tracker.peaks.Workers[i].RSSBytes = max(tracker.peaks.Workers[i].RSSBytes, worker.RSSBytes)
				found = true
				break
			}
		}
		if !found {
			tracker.peaks.Workers = append(tracker.peaks.Workers, opcodeSpikeWorkerRSSPeak{PID: worker.PID, RSSBytes: worker.RSSBytes})
		}
	}
}

func (tracker *opcodeSpikeResourceTracker) sample() (opcodeSpikeResourceSnapshot, error) {
	snapshot, err := tracker.read()
	if err == nil {
		tracker.observe(snapshot)
	}
	return snapshot, err
}

func (tracker *opcodeSpikeResourceTracker) Stop() (opcodeSpikeResourceSnapshot, opcodeSpikeRSSPeaks, opcodeSpikeResourceSnapshot, error) {
	close(tracker.stop)
	<-tracker.done
	final, err := tracker.sample()
	return tracker.initial, tracker.peaks, final, errors.Join(tracker.err, err)
}
